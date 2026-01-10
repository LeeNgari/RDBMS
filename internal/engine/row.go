package engine

// Row represents a single table row
// Key = column name, Value = cell value
type Row map[string]interface{}

// Add this method to your Row type definition
func (r Row) Copy() Row {
	copy := make(Row, len(r))
	for k, v := range r {
		copy[k] = v
	}
	return copy
}
