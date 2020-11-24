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
	"errors"
	"time"
)

// table is an element of the tree which represents inter-table relationships.
type table struct {
	tableName            string
	childTables          []*table
	parentTableName      string
	parentOnDeleteAction deleteActionType
	referencedBy         []*table
	deleter              *deleter
}

// isDeletable returns true if the table is ready to be deleted.
func (t *table) isDeletable() bool {
	for _, child := range t.childTables {
		if child.parentOnDeleteAction == deleteActionNoAction && child.deleter.status != statusCompleted {
			return false
		}
		if !child.isDeletable() {
			return false
		}
	}

	for _, referencing := range t.referencedBy {
		if referencing.deleter.status != statusCompleted {
			return false
		}
	}

	return true
}

// constructTableTree creates a table tree which represents inter-table relationships.
func constructTableTree(originals []*table, parentTableName string) []*table {
	var tables []*table
	for _, original := range originals {
		if original.parentTableName == parentTableName {
			original.childTables = constructTableTree(originals, original.tableName)
			tables = append(tables, original)
		}
	}
	return tables
}

// flattenTables flatten table tree to list of tables.
func flattenTables(tables []*table) []*table {
	var flatten []*table
	for _, table := range tables {
		flatten = append(flatten, table)
		childFlatten := flattenTables(table.childTables)
		flatten = append(flatten, childFlatten...)
	}
	return flatten
}

// findDeletableTables returns tables which can be deleted.
func findDeletableTables(tables []*table) []*table {
	var deletable []*table
	for _, table := range tables {
		if s := table.deleter.status; s == statusDeleting || s == statusCompleted {
			continue
		}
		if table.isDeletable() {
			deletable = append(deletable, table)
			// Parent table will be deleted, so child tables will be also deleted.
			continue
		}

		if len(table.childTables) > 0 {
			childDeletables := findDeletableTables(table.childTables)
			deletable = append(deletable, childDeletables...)
		}
	}

	return deletable
}

// coordinator initiates deleting rows from tables without violating database constraints.
type coordinator struct {
	tables  []*table
	errChan chan error
}

func newCoordinator(schemas []*tableSchema, client *spanner.Client) *coordinator {
	var tables []*table
	tableMap := map[string]*table{}
	for _, schema := range schemas {
		t := &table{
			tableName:            schema.tableName,
			parentTableName:      schema.parentTableName,
			parentOnDeleteAction: schema.parentOnDeleteAction,
			deleter: &deleter{
				tableName: schema.tableName,
				client:    client,
			},
			referencedBy: []*table{},
		}
		tables = append(tables, t)
		tableMap[schema.tableName] = t
	}

	// Construct FK reference relationships.
	for _, schema := range schemas {
		if len(schema.referencedBy) == 0 {
			continue
		}

		table := tableMap[schema.tableName]
		for _, referencing := range schema.referencedBy {
			table.referencedBy = append(table.referencedBy, tableMap[referencing])
		}
	}

	// Construct Parent-Child relationships.
	tables = constructTableTree(tables, "") // root

	return &coordinator{
		tables:  tables,
		errChan: make(chan error),
	}
}

// start starts coordination in another goroutine.
func (c *coordinator) start(ctx context.Context) {
	go func() {
		for _, table := range flattenTables(c.tables) {
			table.deleter.startRowCountUpdater(ctx)
		}

		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				tables := findDeletableTables(c.tables)
				if len(tables) == 0 {
					if !isAllTablesDeleted(c.tables) && !isAnyTableDeleting(c.tables) {
						c.errChan <- errors.New("no deletable tables found, probably there is circular dependencies between tables")
					}
				}

				for _, table := range tables {
					go func() {
						if err := table.deleter.deleteRows(ctx); err != nil {
							c.errChan <- err
						}
					}()
					cascadeDelete(table.childTables)
				}
			case <-ctx.Done():
				c.errChan <- ctx.Err()
			}
		}
	}()
}

// waitCompleted blocks until all deletions are completed.
func (c *coordinator) waitCompleted() error {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			if isAllTablesDeleted(c.tables) {
				return nil
			}
		case err := <-c.errChan:
			if err != nil {
				return err
			}
		}
	}
}

func isAllTablesDeleted(tables []*table) bool {
	for _, table := range tables {
		if table.deleter.status != statusCompleted {
			return false
		}
		if !isAllTablesDeleted(table.childTables) {
			return false
		}
	}
	return true
}

func isAnyTableDeleting(tables []*table) bool {
	for _, table := range tables {
		if table.deleter.status == statusDeleting || table.deleter.status == statusCascadeDeleting {
			return true
		}
		if isAnyTableDeleting(table.childTables) {
			return true
		}
	}
	return false
}

// cascadeDelete marks all of child tables as cascade deleting status.
func cascadeDelete(tables []*table) {
	for _, table := range tables {
		table.deleter.parentDeletionStarted()
		cascadeDelete(table.childTables)
	}
}
