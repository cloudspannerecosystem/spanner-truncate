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
	"time"

	"cloud.google.com/go/spanner"
)

// Status is a delete status.
type status int

const (
	statusAnalyzing       status = iota // Status for calculating the total rows in the table.
	statusWaiting                       // Status for waiting for dependent tables being deleted.
	statusDeleting                      // Status for deleting rows.
	statusCascadeDeleting               // Status for deleting rows by parent in cascaded way.
	statusCompleted                     // Status for delete completed.
)

// deleter deletes all rows from the table.
type deleter struct {
	tableName string
	client    *spanner.Client
	status    status

	// Total rows in the table.
	// Once set, we don't update this number even if new rows are added to the table.
	totalRows uint64

	// Remained rows in the table.
	remainedRows uint64
}

// deleteRows deletes rows from the table using PDML.
func (d *deleter) deleteRows(ctx context.Context) error {
	d.status = statusDeleting
	stmt := spanner.NewStatement(fmt.Sprintf("DELETE FROM `%s` WHERE true", d.tableName))
	_, err := d.client.PartitionedUpdate(ctx, stmt)
	return err
}

func (d *deleter) parentDeletionStarted() {
	d.status = statusCascadeDeleting
}

// startRowCountUpdater starts periodical row count in another goroutine.
func (d *deleter) startRowCountUpdater(ctx context.Context) {
	go func() {
		for {
			if d.status == statusCompleted {
				return
			}

			// Ignore error as it could be a temporal error.
			d.updateRowCount(ctx)

			// Sleep for a while to minimize the impact on CPU usage caused by SELECT COUNT(*) queries.
			time.Sleep(1 * time.Second)
		}
	}()
}

func (d *deleter) updateRowCount(ctx context.Context) error {
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT COUNT(*) as count FROM `%s`", d.tableName))
	var count int64

	// Use stale read to minimize the impact on the leader replica.
	txn := d.client.Single().WithTimestampBound(spanner.ExactStaleness(time.Second))
	if err := txn.Query(ctx, stmt).Do(func(r *spanner.Row) error {
		return r.ColumnByName("count", &count)
	}); err != nil {
		return err
	}

	if d.totalRows == 0 {
		d.totalRows = uint64(count)
	}
	d.remainedRows = uint64(count)

	if count == 0 {
		d.status = statusCompleted
	} else if d.status == statusAnalyzing {
		d.status = statusWaiting
	}

	return nil
}
