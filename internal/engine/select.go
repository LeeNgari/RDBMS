package engine

// SelectAll returns all rows of the table
func SelectAll(t *Table) []Row {

	rows := make([]Row, len(t.Rows))
	copy(rows, t.Rows)
	return rows
}

type PredicateFunc func(Row) bool
func SelectWhere(t *Table, pred PredicateFunc) []Row {
	var result []Row
	for _, row := range t.Rows {
		if pred(row) {
			result = append(result, row)
		}
	}
	return result
}
func SelectByUniqueIndex(t *Table, colName string, value interface{}) (Row, bool) {
    idx, exists := t.Indexes[colName]
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

    return t.Rows[positions[0]], true
}