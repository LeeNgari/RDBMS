package operations

import (
	"github.com/leengari/mini-rdbms/internal/domain/data"
	"github.com/leengari/mini-rdbms/internal/domain/schema"
)

// PredicateFunc is a function that tests whether a row matches certain criteria
type PredicateFunc func(data.Row) bool

// SelectAll returns all rows of the table with optional column projection
// If proj is nil or SelectAll is true, returns all columns
// Otherwise, returns only the columns specified in the projection
func SelectAll(table *schema.Table, proj *Projection) []data.Row {
	table.RLock()
	defer table.RUnlock()

	rows := make([]data.Row, len(table.Rows))
	for i, row := range table.Rows {
		rows[i] = projectRow(row, proj, table.Name)
	}
	return rows
}

// SelectWhere returns rows that match the given predicate with optional column projection
// The predicate is evaluated on the full row, then projection is applied to matching rows
func SelectWhere(table *schema.Table, pred PredicateFunc, proj *Projection) []data.Row {
	table.RLock()
	defer table.RUnlock()

	var result []data.Row
	for _, row := range table.Rows {
		if pred(row) {
			result = append(result, projectRow(row, proj, table.Name))
		}
	}
	return result
}

// SelectByUniqueIndex retrieves a row using a unique index with optional column projection
// Returns the projected row and true if found, nil and false otherwise
func SelectByUniqueIndex(table *schema.Table, colName string, value interface{}, proj *Projection) (data.Row, bool) {
	table.RLock()
	defer table.RUnlock()

	idx, exists := table.Indexes[colName]
	if !exists || !idx.Unique {
		return nil, false
	}

	// Convert value to int64 if it's an integer type for comparison
	if intVal, ok := value.(int); ok {
		value = int64(intVal)
	}

	positions, found := idx.Data[value]
	if !found || len(positions) == 0 {
		return nil, false
	}

	row := table.Rows[positions[0]]
	return projectRow(row, proj, table.Name), true
}
