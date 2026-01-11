package operations

import (
	"fmt"

	"github.com/leengari/mini-rdbms/internal/domain/data"
	"github.com/leengari/mini-rdbms/internal/domain/schema"
)

// ValidateProjection checks if all columns in the projection exist in the table schema
// Returns error if any column is not found
func ValidateProjection(table *schema.Table, proj *Projection) error {
	if proj == nil || proj.SelectAll {
		return nil
	}

	for _, colRef := range proj.Columns {
		// Skip validation for columns from other tables (for JOIN queries)
		if colRef.Table != "" && colRef.Table != table.Name {
			continue
		}

		found := false
		for _, col := range table.Schema.Columns {
			if col.Name == colRef.Column {
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("column '%s' does not exist in table '%s'", colRef.Column, table.Name)
		}
	}

	return nil
}

// projectRow applies projection to a single row
// Returns a new row containing only the requested columns
// If projection is nil or SelectAll is true, returns a copy of the entire row
func projectRow(row data.Row, proj *Projection, tableName string) data.Row {
	// If no projection or SELECT *, return full row
	if proj == nil || proj.SelectAll {
		return row.Copy()
	}

	projected := make(data.Row)

	for _, colRef := range proj.Columns {
		// Skip columns from other tables (for JOIN queries)
		if colRef.Table != "" && colRef.Table != tableName {
			continue
		}

		value, exists := row[colRef.Column]
		if !exists {
			// Column doesn't exist in this row - skip it
			// This can happen in JOIN queries where not all columns are present
			continue
		}

		// Use alias if provided, otherwise use column name
		key := colRef.Column
		if colRef.Alias != "" {
			key = colRef.Alias
		}

		projected[key] = value
	}

	return projected
}

// projectJoinedRow applies projection to a joined row
// Handles table-qualified column names (e.g., "users.id", "orders.product")
func projectJoinedRow(row data.JoinedRow, proj *Projection) data.JoinedRow {
	// If no projection or SELECT *, return full row
	if proj == nil || proj.SelectAll {
		return row
	}

	projected := data.NewJoinedRow()

	for _, colRef := range proj.Columns {
		// Build the qualified column name
		var qualifiedName string
		if colRef.Table != "" {
			qualifiedName = fmt.Sprintf("%s.%s", colRef.Table, colRef.Column)
		} else {
			// If no table qualifier, we need to search for the column
			// This handles ambiguous columns - user should qualify them
			qualifiedName = colRef.Column
		}

		value, exists := row.Get(qualifiedName)
		if !exists && colRef.Table == "" {
			// Try to find the column in any table
			// This is a fallback for unqualified column names
			for key, val := range row.Data {
				if key == colRef.Column || len(key) > len(colRef.Column) && key[len(key)-len(colRef.Column)-1:] == "."+colRef.Column {
					value = val
					exists = true
					break
				}
			}
		}

		if !exists {
			// Column doesn't exist - skip it
			continue
		}

		// Use alias if provided, otherwise use qualified name
		key := qualifiedName
		if colRef.Alias != "" {
			key = colRef.Alias
		}

		projected.Set(key, value)
	}

	return projected
}
