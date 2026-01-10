package engine

type Table struct {
	Name string
	Path string
	Schema *TableSchema
	Rows [] Row
	Indexes map[string] *Index
}