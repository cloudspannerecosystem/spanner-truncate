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
	"cloud.google.com/go/spanner"
	"context"
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

func fetchTableSchemas(ctx context.Context, client *spanner.Client, targetTables []string) ([]*tableSchema, error) {
	// This query fetches the table metadata and relationships.
	sql := `
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
		WHERE T.TABLE_CATALOG = "" AND T.TABLE_SCHEMA = ""
`
	var params map[string]interface{}
	if len(targetTables) > 0 {
		sql += " AND T.TABLE_NAME IN UNNEST(@TargetTables) "
		params = map[string]interface{}{"TargetTables": targetTables}
	}
	sql += " ORDER BY T.TABLE_NAME ASC"
	stmt := spanner.Statement{
		SQL:    sql,
		Params: params,
	}
	iter := client.Single().Query(ctx, stmt)

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
