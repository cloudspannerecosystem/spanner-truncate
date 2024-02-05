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

// indexSchema represents secondary index metadata.
type indexSchema struct {
	indexName string

	// Table name on which the index is defined.
	baseTableName string

	// Table name the index interleaved in. If blank, the index is a global index.
	parentTableName string
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
