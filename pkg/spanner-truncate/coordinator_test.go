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
)

func TestNewCoordinator(t *testing.T) {
	for _, test := range []struct {
		desc    string
		schemas []*tableSchema
		indexes []*indexSchema
		want    []*table
	}{
		{
			desc: "Flat",
			schemas: []*tableSchema{
				{tableName: "A", parentTableName: ""},
				{tableName: "B", parentTableName: ""},
			},
			want: []*table{
				{tableName: "A"},
				{tableName: "B"},
			},
		},
		{
			desc: "Parent-child relationship",
			schemas: []*tableSchema{
				{tableName: "A", parentTableName: ""},
				{tableName: "B", parentTableName: ""},
				{tableName: "C", parentTableName: "B"},
			},
			want: []*table{
				{tableName: "A"},
				{tableName: "B", childTables: []*table{{tableName: "C"}}},
			},
		},
		{
			desc: "Only child table specified",
			schemas: []*tableSchema{
				{tableName: "C", parentTableName: "B"},
			},
			want: []*table{
				{tableName: "C"},
			},
		},
		{
			desc: "Only child table specified in multiple tables",
			schemas: []*tableSchema{
				{tableName: "C", parentTableName: "B"},
				{tableName: "D", parentTableName: "A"},
			},
			want: []*table{
				{tableName: "C"},
				{tableName: "D"},
			},
		},
		{
			desc: "Only child table specified in two levels",
			schemas: []*tableSchema{
				{tableName: "C", parentTableName: "B"},
				{tableName: "D", parentTableName: "C"},
			},
			want: []*table{
				{tableName: "C", childTables: []*table{{tableName: "D"}}},
			},
		},
		{
			desc: "Foreign Key reference",
			schemas: []*tableSchema{
				{tableName: "A", parentTableName: ""},
				{tableName: "B", parentTableName: "", referencedBy: []string{}},
				{tableName: "C", parentTableName: "", referencedBy: []string{"B"}},
			},
			want: []*table{
				{tableName: "A"},
				{tableName: "B"},
				{tableName: "C", referencedBy: []*table{{tableName: "B"}}},
			},
		},
		{
			desc: "Child table has an interleaved index",
			schemas: []*tableSchema{
				{tableName: "A", parentTableName: ""},
				{tableName: "B", parentTableName: "A"},
			},
			indexes: []*indexSchema{
				{indexName: "Bi", baseTableName: "B", parentTableName: "B"},
			},
			want: []*table{
				{tableName: "A", hasGlobalIndex: false, childTables: []*table{{tableName: "B", hasGlobalIndex: false}}},
			},
		},
		{
			desc: "Child table has a global (non-interleaved) index",
			schemas: []*tableSchema{
				{tableName: "A", parentTableName: ""},
				{tableName: "B", parentTableName: "A"},
			},
			indexes: []*indexSchema{
				{indexName: "Bi", baseTableName: "B", parentTableName: ""},
			},
			want: []*table{
				{tableName: "A", hasGlobalIndex: false, childTables: []*table{{tableName: "B", hasGlobalIndex: true}}},
			},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			coordinator := newCoordinator(test.schemas, test.indexes, nil)
			got := coordinator.tables
			if !compareTables(got, test.want) {
				t.Errorf("invalid tables: got = %#v, want = %#v", got, test.want)
			}
		})
	}
}

func TestFindDeletableTables(t *testing.T) {
	for _, tt := range []struct {
		desc       string
		tablesFunc func() []*table
		want       []string
	}{
		{
			desc: "Flatten tables",
			tablesFunc: func() []*table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{}}
				return []*table{tableA, tableB}
			},
			want: []string{"A", "B"},
		},
		{
			desc: "Parent-child tables with cascade-delete",
			tablesFunc: func() []*table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{}}
				tableC := &table{tableName: "C", deleter: &deleter{}}
				tableB.childTables = []*table{tableC}
				tableC.parentTableName = "B"
				tableC.parentOnDeleteAction = deleteActionCascadeDelete
				return []*table{tableA, tableB}
			},
			want: []string{"A", "B"},
		},
		{
			desc: "Parent-child tables with delete-no-action",
			tablesFunc: func() []*table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{}}
				tableC := &table{tableName: "C", deleter: &deleter{}}
				tableB.childTables = []*table{tableC}
				tableC.parentTableName = "B"
				tableC.parentOnDeleteAction = deleteActionNoAction
				return []*table{tableA, tableB}
			},
			want: []string{"A", "C"},
		},
		{
			desc: "Parent-child tables with delete-no-action, but already child was deleted",
			tablesFunc: func() []*table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{}}
				tableC := &table{tableName: "C", deleter: &deleter{status: statusCompleted}}
				tableB.childTables = []*table{tableC}
				tableC.parentTableName = "B"
				tableC.parentOnDeleteAction = deleteActionNoAction
				return []*table{tableA, tableB}
			},
			want: []string{"A", "B"},
		},
		{
			desc: "Parent-child tables with cascade-delete & delete-no-action",
			tablesFunc: func() []*table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{}}
				tableC := &table{tableName: "C", deleter: &deleter{}}
				tableD := &table{tableName: "D", deleter: &deleter{}}

				// A -- B
				tableA.childTables = []*table{tableB}
				tableB.parentTableName = "A"
				tableB.parentOnDeleteAction = deleteActionCascadeDelete

				// B -- C
				tableB.childTables = []*table{tableC}
				tableC.parentTableName = "B"
				tableC.parentOnDeleteAction = deleteActionNoAction

				// C -- D
				tableC.childTables = []*table{tableD}
				tableD.parentTableName = "C"
				tableD.parentOnDeleteAction = deleteActionCascadeDelete

				return []*table{tableA}
			},
			want: []string{"C"},
		},
		{
			desc: "Foreign key references",
			tablesFunc: func() []*table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{}}
				tableA.referencedBy = []*table{tableB}
				return []*table{tableA, tableB}
			},
			want: []string{"B"},
		},
		{
			desc: "Foreign key references, but referencing table was already deleted",
			tablesFunc: func() []*table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{status: statusCompleted}}
				tableA.referencedBy = []*table{tableB}
				return []*table{tableA, tableB}
			},
			want: []string{"A"},
		},
		{
			desc: "Parent-child tables with foreign key references",
			tablesFunc: func() []*table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{}}
				tableC := &table{tableName: "C", deleter: &deleter{}}
				tableD := &table{tableName: "D", deleter: &deleter{}}

				// A -- B
				tableA.childTables = []*table{tableB}
				tableB.parentTableName = "A"
				tableB.parentOnDeleteAction = deleteActionCascadeDelete

				// C -- D
				tableC.childTables = []*table{tableD}
				tableD.parentTableName = "C"
				tableD.parentOnDeleteAction = deleteActionCascadeDelete

				// Foreign key
				tableB.referencedBy = []*table{tableD}

				return []*table{tableA, tableC}
			},
			want: []string{"C"},
		},
		{
			desc: "Child table has a global index",
			tablesFunc: func() []*table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{}}

				tableA.childTables = []*table{tableB}
				tableB.parentTableName = "A"
				tableB.parentOnDeleteAction = deleteActionCascadeDelete

				// Assuming that tableB has a global index
				tableB.hasGlobalIndex = true

				return []*table{tableA}
			},
			want: []string{"B"},
		},
		{
			desc: "Child table has a global index, but already child was deleted",
			tablesFunc: func() []*table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{status: statusCompleted}}

				tableA.childTables = []*table{tableB}
				tableB.parentTableName = "A"
				tableB.parentOnDeleteAction = deleteActionCascadeDelete

				tableB.hasGlobalIndex = true

				return []*table{tableA}
			},
			want: []string{"A"},
		},
		{
			desc: "Parent table has a global index",
			tablesFunc: func() []*table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{}}

				tableA.childTables = []*table{tableB}
				tableB.parentTableName = "A"
				tableB.parentOnDeleteAction = deleteActionCascadeDelete

				// Assuming that tableA (parent table) has a global index.
				tableA.hasGlobalIndex = true

				return []*table{tableA}
			},
			want: []string{"A"}, // it shouldn't block parent deletion
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			got := findDeletableTables(tt.tablesFunc())
			gotNames := extractTableNames(got)
			if !cmp.Equal(gotNames, tt.want) {
				t.Errorf("diff(+got, -want) = %v", cmp.Diff(gotNames, tt.want))
			}
		})
	}
}

func TestTableIsDeletable(t *testing.T) {
	for _, tt := range []struct {
		desc      string
		tableFunc func() *table
		want      bool
	}{
		{
			desc: "Simple table",
			tableFunc: func() *table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				return tableA
			},
			want: true,
		},
		{
			desc: "Child table has cascade-delete action",
			tableFunc: func() *table {
				parent := &table{tableName: "Parent", deleter: &deleter{}}
				child := &table{tableName: "Child", deleter: &deleter{}}
				parent.childTables = []*table{child}
				child.parentTableName = "Parent"
				child.parentOnDeleteAction = deleteActionCascadeDelete
				return parent
			},
			want: true,
		},
		{
			desc: "Child table has no-action action",
			tableFunc: func() *table {
				parent := &table{tableName: "Parent", deleter: &deleter{}}
				child := &table{tableName: "Child", deleter: &deleter{}}
				parent.childTables = []*table{child}
				child.parentTableName = "Parent"
				child.parentOnDeleteAction = deleteActionNoAction
				return parent
			},
			want: false,
		},
		{
			desc: "Child table has no-action action, but it was already deleted",
			tableFunc: func() *table {
				parent := &table{tableName: "Parent", deleter: &deleter{}}
				child := &table{tableName: "Child", deleter: &deleter{status: statusCompleted}}
				parent.childTables = []*table{child}
				child.parentTableName = "Parent"
				child.parentOnDeleteAction = deleteActionNoAction
				return parent
			},
			want: true,
		},
		{
			desc: "Foreign key references",
			tableFunc: func() *table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{}}
				tableA.referencedBy = []*table{tableB}
				return tableA
			},
			want: false,
		},
		{
			desc: "Foreign key references, but referencing table was already deleted",
			tableFunc: func() *table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{status: statusCompleted}}
				tableA.referencedBy = []*table{tableB}
				return tableA
			},
			want: true,
		},
		{
			desc: "Child table has a global index",
			tableFunc: func() *table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{}}
				tableA.childTables = []*table{tableB}
				tableB.hasGlobalIndex = true
				return tableA
			},
			want: false,
		},
		{
			desc: "Child table has a global index, but the child table was already deleted",
			tableFunc: func() *table {
				tableA := &table{tableName: "A", deleter: &deleter{}}
				tableB := &table{tableName: "B", deleter: &deleter{status: statusCompleted}}
				tableA.childTables = []*table{tableB}
				tableB.hasGlobalIndex = true
				return tableA
			},
			want: true,
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			table := tt.tableFunc()
			if got := table.isDeletable(); got != tt.want {
				t.Errorf("isDeletable(%v) = %v, but want = %v", table, got, tt.want)
			}
		})
	}
}

func extractTableNames(tables []*table) []string {
	names := make([]string, len(tables))
	for i, table := range tables {
		names[i] = table.tableName
	}
	return names
}

func compareTables(tables1, tables2 []*table) bool {
	if len(tables1) != len(tables2) {
		return false
	}
	for i := 0; i < len(tables1); i++ {
		t1 := tables1[i]
		t2 := tables2[i]
		if t1.tableName != t2.tableName {
			return false
		}
		if t1.hasGlobalIndex != t2.hasGlobalIndex {
			return false
		}
		if !compareTables(t1.childTables, t2.childTables) {
			return false
		}
		if !compareTables(t1.referencedBy, t2.referencedBy) {
			return false
		}
	}
	return true
}
