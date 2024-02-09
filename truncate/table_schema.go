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
	"context"
	"errors"

	"cloud.google.com/go/spanner"
)

// deleteActionType is action type on parent delete.
type deleteActionType int

const (
	deleteActionUndefined     deleteActionType = iota // Undefined action type on parent delete.
	deleteActionCascadeDelete                         // Cascade delete type on parent delete.
	deleteActionNoAction                              // No action type on parent delete.
)

// tableSchema represents table metadata and relationships.
type tableSchema struct {
	tableName string

	// Parent / Child relationship.
	parentTableName      string
	parentOnDeleteAction deleteActionType

	// Foreign Key Reference.
	referencedBy []string
}

func (t *tableSchema) isCascadeDeletable() bool {
	return t.parentOnDeleteAction == deleteActionCascadeDelete
}

func (t *tableSchema) isRoot() bool {
	return t.parentTableName == ""
}

// indexSchema represents secondary index metadata.
type indexSchema struct {
	indexName string

	// Table name on which the index is defined.
	baseTableName string

	// Table name the index interleaved in. If blank, the index is a global index.
	parentTableName string
}

// tableLineage represents a table schema and its ancestors and descendants.
// This is used to represent inter-table relationships.
// The order of ancestors and descendants are not guaranteed.
type tableLineage struct {
	tableSchema *tableSchema
	ancestors   []*tableSchema
	descendants []*tableSchema
}

// fetchTableSchemas fetches schema information from spanner database.
func fetchTableSchemas(ctx context.Context, client *spanner.Client) ([]*tableSchema, error) {
	// This query fetches the table metadata and relationships.
	iter := client.Single().Query(ctx, spanner.NewStatement(`
		WITH FKReferences AS (
			SELECT CCU.TABLE_NAME AS Referenced, ARRAY_AGG(TC.TABLE_NAME) AS Referencing
			FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS as TC
			INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU ON TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME
			WHERE TC.TABLE_CATALOG = '' AND TC.TABLE_SCHEMA = '' AND TC.CONSTRAINT_TYPE = 'FOREIGN KEY' AND CCU.TABLE_CATALOG = '' AND CCU.TABLE_SCHEMA = ''
			GROUP BY CCU.TABLE_NAME
		)
		SELECT T.TABLE_NAME, T.PARENT_TABLE_NAME, T.ON_DELETE_ACTION, IF(F.Referencing IS NULL, ARRAY<STRING>[], F.Referencing) AS referencedBy
		FROM INFORMATION_SCHEMA.TABLES AS T
		LEFT OUTER JOIN FKReferences AS F ON T.TABLE_NAME = F.Referenced
		WHERE T.TABLE_CATALOG = "" AND T.TABLE_SCHEMA = "" AND T.TABLE_TYPE = "BASE TABLE"
		ORDER BY T.TABLE_NAME ASC
	`))

	var tables []*tableSchema
	if err := iter.Do(func(r *spanner.Row) error {
		var (
			tableName    string
			parent       spanner.NullString
			deleteAction spanner.NullString
			referencedBy []string
		)
		if err := r.Columns(&tableName, &parent, &deleteAction, &referencedBy); err != nil {
			return err
		}

		var parentTableName string
		if parent.Valid {
			parentTableName = parent.StringVal
		}

		var typ deleteActionType
		if deleteAction.Valid {
			switch deleteAction.StringVal {
			case "CASCADE":
				typ = deleteActionCascadeDelete
			case "NO ACTION":
				typ = deleteActionNoAction
			}
		}

		tables = append(tables, &tableSchema{
			tableName:            tableName,
			parentTableName:      parentTableName,
			parentOnDeleteAction: typ,
			referencedBy:         referencedBy,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	return tables, nil
}

// filterTableSchemas filters tables with given targetTables and excludeTables.
// If targetTables is not empty, it fetches only the specified tables.
// If excludeTables is not empty, it excludes the specified tables.
// TargetTables and excludeTables cannot be specified at the same time.
func filterTableSchemas(tables []*tableSchema, targetTables, excludeTables []string) ([]*tableSchema, error) {
	isExclude := len(excludeTables) > 0
	isTarget := len(targetTables) > 0

	switch {
	case isTarget && isExclude:
		return nil, errors.New("both targetTables and excludeTables cannot be specified at the same time")
	case isTarget:
		return targetFilterTableSchemas(tables, targetTables), nil
	case isExclude:
		return excludeFilterTableSchemas(tables, excludeTables), nil
	default: // No target and exclude tables are specified.
		return tables, nil
	}
}

// targetFilterTableSchemas filters tables with given targetTables.
// If targetTables is empty, it returns all tables.
func targetFilterTableSchemas(tables []*tableSchema, targetTables []string) []*tableSchema {
	if len(targetTables) == 0 {
		return tables
	}

	isTarget := make(map[string]bool, len(tables))
	for _, t := range targetTables {
		isTarget[t] = true
	}

	// TODO: Add child tables that may be deleted in cascade (#18)

	filtered := make([]*tableSchema, 0, len(tables))
	for _, t := range tables {
		if isTarget[t.tableName] {
			filtered = append(filtered, t)
		}
	}

	return filtered
}

// excludeFilterTableSchemas filters tables with given excludeTables.
// If excludeTables is empty, it returns all tables.
// When an exclude table is cascade deletable, its parent table is also excluded.
func excludeFilterTableSchemas(tables []*tableSchema, excludeTableSchemas []string) []*tableSchema {
	if len(excludeTableSchemas) == 0 {
		return tables
	}

	isExclude := make(map[string]bool, len(tables))
	for _, t := range excludeTableSchemas {
		isExclude[t] = true
	}

	// Additionally exclude parent tables that may delete the exclude tables in cascade
	lineages := constructTableLineages(tables)
	for _, l := range lineages {
		if isExclude[l.tableSchema.tableName] {
			for _, a := range l.ancestors {
				isExclude[a.tableName] = true
			}
		}
	}

	filtered := make([]*tableSchema, 0, len(tables))
	for _, t := range tables {
		if !isExclude[t.tableName] {
			filtered = append(filtered, t)
		}
	}

	return filtered
}

// constructTableLineages returns a list of interleave Lineages.
// This function creates tableLineage for each of all given tableSchemas.
func constructTableLineages(tables []*tableSchema) []*tableLineage {
	tableMap := make(map[string]*tableSchema, len(tables))
	for _, t := range tables {
		tableMap[t.tableName] = t
	}

	parentRelation := make(map[string]*tableSchema, len(tables))
	for _, t := range tables {
		if !t.isRoot() && t.isCascadeDeletable() {
			parentRelation[t.tableName] = tableMap[t.parentTableName]
		}
	}

	childRelation := make(map[string][]*tableSchema, len(tables))
	for _, t := range tables {
		if !t.isRoot() && t.isCascadeDeletable() {
			childRelation[t.parentTableName] = append(childRelation[t.parentTableName], t)
		}
	}

	lineages := make([]*tableLineage, 0, len(tables))
	for _, t := range tables {
		lineages = append(lineages, &tableLineage{
			tableSchema: t,
			ancestors:   findAncestors(t, parentRelation, nil),
			descendants: findDescendants(t, childRelation, nil),
		})
	}

	return lineages
}

// findAncestors recursively finds all ancestors of the given table .
func findAncestors(table *tableSchema, parentRelation map[string]*tableSchema, ancestors []*tableSchema) []*tableSchema {
	if parent, ok := parentRelation[table.tableName]; ok {
		ancestors = append(ancestors, parent)
		return findAncestors(parent, parentRelation, ancestors)
	}
	return ancestors
}

// findDescendants recursively finds all descendants of the given table.
func findDescendants(table *tableSchema, childRelation map[string][]*tableSchema, descendants []*tableSchema) []*tableSchema {
	for _, child := range childRelation[table.tableName] {
		descendants = append(descendants, child)
		descendants = findDescendants(child, childRelation, descendants)
	}
	return descendants
}

func fetchIndexSchemas(ctx context.Context, client *spanner.Client) ([]*indexSchema, error) {
	// This query fetches defined indexes.
	iter := client.Single().Query(ctx, spanner.NewStatement(`
		SELECT INDEX_NAME, TABLE_NAME, PARENT_TABLE_NAME FROM INFORMATION_SCHEMA.INDEXES
		WHERE INDEX_TYPE = 'INDEX' AND TABLE_CATALOG = '' AND TABLE_SCHEMA = '';
	`))

	var indexes []*indexSchema
	if err := iter.Do(func(r *spanner.Row) error {
		var (
			indexName     string
			baseTableName string
			parent        spanner.NullString
		)
		if err := r.Columns(&indexName, &baseTableName, &parent); err != nil {
			return err
		}

		var parentTableName string
		if parent.Valid {
			parentTableName = parent.StringVal
		}

		indexes = append(indexes, &indexSchema{
			indexName:       indexName,
			baseTableName:   baseTableName,
			parentTableName: parentTableName,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	return indexes, nil
}
