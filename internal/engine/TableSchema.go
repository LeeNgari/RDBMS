package engine

// TableSchema represents table metadata (from meta.json)
type TableSchema struct {
	TableName string
	Columns   []Column
}