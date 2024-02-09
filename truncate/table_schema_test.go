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

package truncate

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestTargetFilterTableSchemas(t *testing.T) {
	var (
		// The following tables are flat schemas and not related to each other.
		t1 = &tableSchema{
			tableName:            "t1",
			parentTableName:      "",
			parentOnDeleteAction: deleteActionUndefined,
			referencedBy:         nil,
		}
		t2 = &tableSchema{
			tableName:            "t2",
			parentTableName:      "",
			parentOnDeleteAction: deleteActionUndefined,
			referencedBy:         nil,
		}
		t3 = &tableSchema{
			tableName:            "t3",
			parentTableName:      "",
			parentOnDeleteAction: deleteActionUndefined,
			referencedBy:         nil,
		}
	)

	opts := []cmp.Option{
		cmpopts.SortSlices(func(i, j *tableSchema) bool {
			return i.tableName < j.tableName
		}),
		cmp.AllowUnexported(tableSchema{}),
	}

	for _, test := range []struct {
		desc         string
		schemas      []*tableSchema
		targetTables []string
		want         []*tableSchema
	}{
		{
			desc:         "Include multiple tables",
			schemas:      []*tableSchema{t1, t2, t3},
			targetTables: []string{t1.tableName, t2.tableName},
			want:         []*tableSchema{t1, t2},
		},
		{
			desc:         "Do nothing when no target tables are passed.",
			schemas:      []*tableSchema{t1, t2, t3},
			targetTables: nil,
			want:         []*tableSchema{t1, t2, t3},
		},
		// TODO: Determine the specifications for parent-child relationships in hierarchical interleaved tables, and add corresponding tests and implementation.
		// This includes defining the behavior of targetFilterTableSchemas for cases where tables not included in the target list are subject to cascade deletion.
	} {
		t.Run(test.desc, func(t *testing.T) {
			got := targetFilterTableSchemas(test.schemas, test.targetTables)

			if len(got) != len(test.want) {
				t.Errorf("len(got) %d, len(want) %d", len(got), len(test.want))
			}

			if diff := cmp.Diff(got, test.want, opts...); diff != "" {
				t.Errorf("mismatch (-got +want):\n%s", diff)
			}
		})
	}
}

func TestExcludeFilterTableSchemas(t *testing.T) {
	var (
		// The following tables are hierarchical schemas.
		// The table schemas are well known in Cloud Spanner document about 'schema and data model'.
		singers = &tableSchema{
			tableName:            "Singers",
			parentTableName:      "",
			parentOnDeleteAction: deleteActionUndefined,
			referencedBy:         nil,
		}
		albums = &tableSchema{
			tableName:            "Albums",
			parentTableName:      "Singers",
			parentOnDeleteAction: deleteActionCascadeDelete,
			referencedBy:         nil,
		}
		songs = &tableSchema{
			tableName:            "Songs",
			parentTableName:      "Albums",
			parentOnDeleteAction: deleteActionCascadeDelete,
			referencedBy:         nil,
		}

		// The following tables are flat schemas and not related to each other.
		t1 = &tableSchema{
			tableName:            "t1",
			parentTableName:      "",
			parentOnDeleteAction: deleteActionUndefined,
			referencedBy:         nil,
		}
		t2 = &tableSchema{
			tableName:            "t2",
			parentTableName:      "",
			parentOnDeleteAction: deleteActionUndefined,
			referencedBy:         nil,
		}
		t3 = &tableSchema{
			tableName:            "t3",
			parentTableName:      "",
			parentOnDeleteAction: deleteActionUndefined,
			referencedBy:         nil,
		}
	)

	opts := []cmp.Option{
		cmpopts.SortSlices(func(i, j *tableSchema) bool {
			return i.tableName < j.tableName
		}),
		cmp.AllowUnexported(tableSchema{}),
	}

	for _, test := range []struct {
		desc          string
		schemas       []*tableSchema
		excludeTables []string
		want          []*tableSchema
	}{
		{
			desc:          "Exclude the parent tables by tracing up to the topmost level.",
			schemas:       []*tableSchema{singers, albums, songs, t1, t2, t3},
			excludeTables: []string{songs.tableName},
			want:          []*tableSchema{t1, t2, t3},
		},
		{
			desc:          "Exclude only the higher levels without the lower levels.",
			schemas:       []*tableSchema{singers, albums, songs, t1, t2, t3},
			excludeTables: []string{albums.tableName},
			want:          []*tableSchema{songs, t1, t2, t3},
		},
		{
			desc:          "Exclude multiple tables.",
			schemas:       []*tableSchema{singers, albums, songs, t1, t2, t3},
			excludeTables: []string{songs.tableName, t1.tableName, t2.tableName},
			want:          []*tableSchema{t3},
		},
		{
			desc:          "Do nothing when no exclude tables are passed.",
			schemas:       []*tableSchema{singers, albums, songs, t1, t2, t3},
			excludeTables: nil,
			want:          []*tableSchema{singers, albums, songs, t1, t2, t3},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			got := excludeFilterTableSchemas(test.schemas, test.excludeTables)

			if len(got) != len(test.want) {
				t.Errorf("len(got) %d, len(want) %d", len(got), len(test.want))
			}

			if diff := cmp.Diff(got, test.want, opts...); diff != "" {
				t.Errorf("mismatch (-got +want):\n%s", diff)
			}
		})
	}
}
