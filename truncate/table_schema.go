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

const fetchTableSchemasQuery = `
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
`

// fetchTableSchemas fetches schema information from spanner database.
// If targetTables is not empty, it fetches only the specified tables.
func fetchTableSchemas(ctx context.Context, client *spanner.Client, targetTables []string) ([]*tableSchema, error) {
	// This query fetches the table metadata and relationships.
	iter := client.Single().Query(ctx, spanner.NewStatement(fetchTableSchemasQuery))

	fetchAll := true
	targets := make(map[string]bool, len(targetTables))
	if len(targetTables) > 0 {
		fetchAll = false
		for _, t := range targetTables {
			targets[t] = true
		}
	}

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

		if !fetchAll {
			if _, ok := targets[tableName]; !ok {
				return nil
			}
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

// filterTableSchemas filters tables by excludeTables.
// If an exclude table is cascade deletable, its parent table is also excluded.
func filterTableSchemas(tables []*tableSchema, excludeTables []string) []*tableSchema {
	if len(excludeTables) == 0 {
		return tables
	}

	excludes := make(map[string]struct{}, len(tables))
	for _, t := range excludeTables {
		excludes[t] = struct{}{}
	}

	// Add parent tables that may delete the exclude tables in cascade
	// Since interleave tables can be hierarchical, tracing up to the top level is needed.
	for {
		excludeParents := make(map[string]struct{}, len(excludes))
		for _, t := range tables {
			if _, ok := excludes[t.tableName]; ok && t.isCascadeDeletable() {
				if _, alreadyExcluded := excludes[t.parentTableName]; !alreadyExcluded {
					excludeParents[t.parentTableName] = struct{}{}
				}
			}
		}

		// loop until no more parent tables to exclude
		if len(excludeParents) == 0 {
			break
		}

		for p := range excludeParents {
			excludes[p] = struct{}{}
		}
	}

	filtered := make([]*tableSchema, 0, len(tables))
	for _, t := range tables {
		if _, ok := excludes[t.tableName]; !ok {
			filtered = append(filtered, t)
		}
	}

	return filtered
}
