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
	"reflect"
	"sort"
	"testing"
)

func TestFilterTableSchemas(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		schemas       []*tableSchema
		excludeTables []string
		want          []*tableSchema
	}{
		"Exclude the parent tables by tracing up to the topmost level.": {
			schemas:       []*tableSchema{singers, albums, songs, t1, t2, t3},
			excludeTables: []string{songs.tableName},
			want:          []*tableSchema{t1, t2, t3},
		},
		"Exclude only the higher levels without the lower levels.": {
			schemas:       []*tableSchema{singers, albums, songs, t1, t2, t3},
			excludeTables: []string{albums.tableName},
			want:          []*tableSchema{songs, t1, t2, t3},
		},
		"Exclude multiple tables.": {
			schemas:       []*tableSchema{singers, albums, songs, t1, t2, t3},
			excludeTables: []string{songs.tableName, t1.tableName, t2.tableName},
			want:          []*tableSchema{t3},
		},
		"Do nothing when no exclude tables are passed.": {
			schemas:       []*tableSchema{singers, albums, songs, t1, t2, t3},
			excludeTables: nil,
			want:          []*tableSchema{singers, albums, songs, t1, t2, t3},
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := filterTableSchemas(tc.schemas, tc.excludeTables)

			if len(got) != len(tc.want) {
				t.Errorf("len(got) %d, len(want) %d", len(got), len(tc.want))
			}

			sort.Slice(got, func(i, j int) bool {
				return got[i].tableName < got[j].tableName
			})
			sort.Slice(tc.want, func(i, j int) bool {
				return tc.want[i].tableName < tc.want[j].tableName
			})

			for i, w := range tc.want {
				if !reflect.DeepEqual(*got[i], *w) {
					t.Errorf("got %v, want %v", got[i], w)
				}
			}
		})
	}

}

// The table schemas are well known in Cloud Spanner document about 'schema and data model'.
var (
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
