//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const (
	envTestProjectID  = "SPANNER_TRUNCATE_INTEGRATION_TEST_PROJECT_ID"
	envTestInstanceID = "SPANNER_TRUNCATE_INTEGRATION_TEST_INSTANCE_ID"
	envTestDatabaseID = "SPANNER_TRUNCATE_INTEGRATION_TEST_DATABASE_ID"
)

var (
	skipIntegrateTest bool

	testProjectID  string
	testInstanceID string
	testDatabaseID string

	tableIDCounter uint32
)

func TestMain(m *testing.M) {
	initialize()
	os.Exit(m.Run())
}

func initialize() {
	if os.Getenv(envTestProjectID) == "" || os.Getenv(envTestInstanceID) == "" || os.Getenv(envTestDatabaseID) == "" {
		skipIntegrateTest = true
		return
	}

	testProjectID = os.Getenv(envTestProjectID)
	testInstanceID = os.Getenv(envTestInstanceID)
	testDatabaseID = os.Getenv(envTestDatabaseID)
}

func setup(t *testing.T, ctx context.Context, ddls, dmls []string) *spanner.Client {
	adminClient, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("failed to create spanner admin client: %v", err)
	}

	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", testProjectID, testInstanceID, testDatabaseID)
	client, err := spanner.NewClientWithConfig(ctx, dbPath, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened: 10,
			MaxOpened: 10,
		},
	})
	if err != nil {
		t.Fatalf("failed to create spanner client: %v", err)
	}

	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: ddls,
	})
	if err != nil {
		t.Fatalf("failed to update database DDL: %v", err)
	}
	if err := op.Wait(ctx); err != nil {
		t.Fatalf("failed to wait for updating database DDL: %v", err)
	}

	for _, dml := range dmls {
		_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			_, err = txn.Update(ctx, spanner.NewStatement(dml))
			return err
		})
		if err != nil {
			t.Fatalf("failed to apply DML %q: %v", dml, err)
		}
	}

	return client
}

func tearDown(t *testing.T, ctx context.Context, ddls []string) {
	adminClient, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("failed to create spanner admin client: %v", err)
	}

	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", testProjectID, testInstanceID, testDatabaseID)
	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: ddls,
	})
	if err != nil {
		t.Fatalf("failed to update database DDL: %v", err)
	}
	if err := op.Wait(ctx); err != nil {
		t.Fatalf("failed to wait for updating database DDL: %v", err)
	}
}

func generateUniqueTableID() string {
	count := atomic.AddUint32(&tableIDCounter, 1)
	return fmt.Sprintf("spanner_truncate_test_%d_%d", time.Now().Unix(), count)
}

// NOTE that this integration test doesn't work for Cloud Spanner Emulator
// since INFORMATION_SCHEMA.TABLE_CONSTRAINTS table is not available in the emulator.
func TestIntegrationTest(t *testing.T) {
	if skipIntegrateTest {
		t.Skip("skip integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	table1 := generateUniqueTableID()
	table2 := generateUniqueTableID()
	table3 := generateUniqueTableID()
	table4 := generateUniqueTableID()

	// NOTE: Spanner doesn't allow to use trailer ";" in DDL.
	ddls := []string{
		fmt.Sprintf(`CREATE TABLE %s (
  Id INT64 NOT NULL,
  StrCol STRING(16),
  BoolCol BOOL,
  BytesCol BYTES(16),
  TimestampCol TIMESTAMP,
  DateCol DATE,
  ArrayCol ARRAY<INT64>,
) PRIMARY KEY(Id)`, table1),

		fmt.Sprintf(`CREATE TABLE %s (
  T2Id INT64 NOT NULL,
) PRIMARY KEY(T2Id)`, table2),

		fmt.Sprintf(`CREATE TABLE %s (
  T2Id INT64 NOT NULL,
  T3Id INT64 NOT NULL,
) PRIMARY KEY(T2Id, T3Id),
  INTERLEAVE IN PARENT %s ON DELETE CASCADE`, table3, table2),

		fmt.Sprintf(`CREATE TABLE %s (
  T2Id INT64 NOT NULL,
  T3Id INT64 NOT NULL,
  T4Id INT64 NOT NULL,
) PRIMARY KEY(T2Id, T3Id, T4Id),
  INTERLEAVE IN PARENT %s ON DELETE CASCADE`, table4, table3),
	}

	dmls := []string{
		fmt.Sprintf("INSERT INTO `%s` (`Id`, `StrCol`, `BoolCol`, `BytesCol`, `TimestampCol`, `DateCol`, `ArrayCol`) VALUES (1, \"foo\", true, b\"\\x61\\x62\\x63\", TIMESTAMP \"2020-01-23T03:00:00Z\", DATE \"2020-01-23\", [1, 2, 3]);", table1),
		fmt.Sprintf("INSERT INTO `%s` (`Id`, `StrCol`, `BoolCol`, `BytesCol`, `TimestampCol`, `DateCol`, `ArrayCol`) VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL);", table1),
		fmt.Sprintf("INSERT INTO `%s` (`T2Id`) VALUES (1);", table2),
		fmt.Sprintf("INSERT INTO `%s` (`T2Id`) VALUES (2);", table2),
		fmt.Sprintf("INSERT INTO `%s` (`T2Id`) VALUES (3);", table2),
		fmt.Sprintf("INSERT INTO `%s` (`T2Id`, `T3Id`) VALUES (1, 1);", table3),
		fmt.Sprintf("INSERT INTO `%s` (`T2Id`, `T3Id`) VALUES (2, 2);", table3),
		fmt.Sprintf("INSERT INTO `%s` (`T2Id`, `T3Id`) VALUES (3, 3);", table3),
		fmt.Sprintf("INSERT INTO `%s` (`T2Id`, `T3Id`, `T4Id`) VALUES (1, 1, 1);", table4),
		fmt.Sprintf("INSERT INTO `%s` (`T2Id`, `T3Id`, `T4Id`) VALUES (2, 2, 2);", table4),
		fmt.Sprintf("INSERT INTO `%s` (`T2Id`, `T3Id`, `T4Id`) VALUES (3, 3, 3);", table4),
	}
	client := setup(t, ctx, ddls, dmls)
	defer tearDown(t, ctx, []string{
		fmt.Sprintf("DROP TABLE %s", table1),
		// NOTE that we have to delete child tables first.
		fmt.Sprintf("DROP TABLE %s", table4),
		fmt.Sprintf("DROP TABLE %s", table3),
		fmt.Sprintf("DROP TABLE %s", table2),
	})

	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("failed to open /dev/null: %v", err)
	}
	if err := run(ctx, testProjectID, testInstanceID, testDatabaseID, true, devNull, nil, nil); err != nil {
		t.Fatalf("run spanner-truncate failed: %v", err)
	}

	for _, table := range []string{table1, table2, table3, table4} {
		iter := client.Single().Query(ctx, spanner.NewStatement(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)))
		if err := iter.Do(func(r *spanner.Row) error {
			var count int64
			if err := r.Column(0, &count); err != nil {
				return err
			}
			if count != 0 {
				t.Errorf("deleted all rows from %q table, but %d rows remained", table, count)
			}
			return nil
		}); err != nil {
			t.Errorf("failed to count rows: %v", err)
		}
	}
}
