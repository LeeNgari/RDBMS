package engine

// Database represents a single database on disk
// (a directory containing tables subdirectories)

type Database struct {
	Name  string
	Path  string // filesystem path to database directory
	Tables map[string] *Table
}