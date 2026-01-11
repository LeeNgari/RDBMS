package operations_test

import (
	"testing"

	"github.com/leengari/mini-rdbms/internal/domain/data"
	"github.com/leengari/mini-rdbms/internal/domain/schema"
	"github.com/leengari/mini-rdbms/internal/query/operations"
)

// Helper function to create a test table
func createTestTable(name string) *schema.Table {
	table := &schema.Table{
		Name: name,
		Schema: &schema.TableSchema{
			TableName: name,
			Columns: []schema.Column{
				{Name: "id", Type: schema.ColumnTypeInt, PrimaryKey: true, NotNull: true},
				{Name: "name", Type: schema.ColumnTypeText, NotNull: true},
				{Name: "email", Type: schema.ColumnTypeText},
				{Name: "age", Type: schema.ColumnTypeInt},
			},
		},
		Rows:    []data.Row{},
		Indexes: make(map[string]*data.Index),
	}
	return table
}

// TestProjection_SelectAll tests selecting all columns
func TestProjection_SelectAll(t *testing.T) {
	table := createTestTable("users")
	table.Rows = []data.Row{
		{"id": int64(1), "name": "Alice", "email": "alice@example.com", "age": int64(30)},
		{"id": int64(2), "name": "Bob", "email": "bob@example.com", "age": int64(25)},
	}

	// SELECT * (all columns)
	proj := operations.NewProjection()
	results := operations.SelectAll(table, proj)

	if len(results) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(results))
	}

	// Check that all columns are present
	if len(results[0]) != 4 {
		t.Errorf("Expected 4 columns, got %d", len(results[0]))
	}
}

// TestProjection_SelectSpecificColumns tests selecting specific columns
func TestProjection_SelectSpecificColumns(t *testing.T) {
	table := createTestTable("users")
	table.Rows = []data.Row{
		{"id": int64(1), "name": "Alice", "email": "alice@example.com", "age": int64(30)},
		{"id": int64(2), "name": "Bob", "email": "bob@example.com", "age": int64(25)},
	}

	// SELECT id, name
	proj := operations.NewProjectionWithColumns(
		operations.ColumnRef{Column: "id"},
		operations.ColumnRef{Column: "name"},
	)

	results := operations.SelectAll(table, proj)

	if len(results) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(results))
	}

	// Check that only 2 columns are present
	if len(results[0]) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(results[0]))
	}

	// Check that correct columns are present
	if _, exists := results[0]["id"]; !exists {
		t.Error("Expected 'id' column to be present")
	}
	if _, exists := results[0]["name"]; !exists {
		t.Error("Expected 'name' column to be present")
	}
	if _, exists := results[0]["email"]; exists {
		t.Error("Did not expect 'email' column to be present")
	}
}

// TestProjection_WithAlias tests column aliasing
func TestProjection_WithAlias(t *testing.T) {
	table := createTestTable("users")
	table.Rows = []data.Row{
		{"id": int64(1), "name": "Alice", "email": "alice@example.com"},
	}

	// SELECT id AS user_id, name AS username
	proj := operations.NewProjectionWithColumns(
		operations.ColumnRef{Column: "id", Alias: "user_id"},
		operations.ColumnRef{Column: "name", Alias: "username"},
	)

	results := operations.SelectAll(table, proj)

	if len(results) != 1 {
		t.Errorf("Expected 1 row, got %d", len(results))
	}

	// Check that aliases are used
	if _, exists := results[0]["user_id"]; !exists {
		t.Error("Expected 'user_id' alias to be present")
	}
	if _, exists := results[0]["username"]; !exists {
		t.Error("Expected 'username' alias to be present")
	}
	if _, exists := results[0]["id"]; exists {
		t.Error("Did not expect original 'id' column name")
	}
}

// TestProjection_SelectWhere tests projection with WHERE clause
func TestProjection_SelectWhere(t *testing.T) {
	table := createTestTable("users")
	table.Rows = []data.Row{
		{"id": int64(1), "name": "Alice", "age": int64(30)},
		{"id": int64(2), "name": "Bob", "age": int64(25)},
		{"id": int64(3), "name": "Charlie", "age": int64(35)},
	}

	// SELECT name WHERE age > 26
	proj := operations.NewProjectionWithColumns(
		operations.ColumnRef{Column: "name"},
	)

	predicate := func(row data.Row) bool {
		age, ok := row["age"].(int64)
		return ok && age > 26
	}

	results := operations.SelectWhere(table, predicate, proj)

	if len(results) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(results))
	}

	// Check that only 'name' column is present
	if len(results[0]) != 1 {
		t.Errorf("Expected 1 column, got %d", len(results[0]))
	}

	if _, exists := results[0]["name"]; !exists {
		t.Error("Expected 'name' column to be present")
	}
}

// TestProjection_ValidateProjection tests projection validation
func TestProjection_ValidateProjection(t *testing.T) {
	table := createTestTable("users")

	// Valid projection
	validProj := operations.NewProjectionWithColumns(
		operations.ColumnRef{Column: "id"},
		operations.ColumnRef{Column: "name"},
	)

	err := operations.ValidateProjection(table, validProj)
	if err != nil {
		t.Errorf("Expected no error for valid projection, got: %v", err)
	}

	// Invalid projection (non-existent column)
	invalidProj := operations.NewProjectionWithColumns(
		operations.ColumnRef{Column: "nonexistent"},
	)

	err = operations.ValidateProjection(table, invalidProj)
	if err == nil {
		t.Error("Expected error for invalid projection")
	}
}

// TestProjection_SelectByUniqueIndex tests projection with index lookup
func TestProjection_SelectByUniqueIndex(t *testing.T) {
	table := createTestTable("users")
	table.Rows = []data.Row{
		{"id": int64(1), "name": "Alice", "email": "alice@example.com"},
		{"id": int64(2), "name": "Bob", "email": "bob@example.com"},
	}

	// Create index on id
	table.Indexes["id"] = &data.Index{
		Column: "id",
		Unique: true,
		Data: map[interface{}][]int{
			int64(1): {0},
			int64(2): {1},
		},
	}

	// SELECT name WHERE id = 1
	proj := operations.NewProjectionWithColumns(
		operations.ColumnRef{Column: "name"},
	)

	row, found := operations.SelectByUniqueIndex(table, "id", int64(1), proj)

	if !found {
		t.Error("Expected to find row")
	}

	if len(row) != 1 {
		t.Errorf("Expected 1 column, got %d", len(row))
	}

	if name, ok := row["name"].(string); !ok || name != "Alice" {
		t.Errorf("Expected name 'Alice', got %v", row["name"])
	}
}

// TestProjection_EmptyProjection tests nil projection (returns all columns)
func TestProjection_EmptyProjection(t *testing.T) {
	table := createTestTable("users")
	table.Rows = []data.Row{
		{"id": int64(1), "name": "Alice", "email": "alice@example.com"},
	}

	// nil projection should return all columns
	results := operations.SelectAll(table, nil)

	if len(results) != 1 {
		t.Errorf("Expected 1 row, got %d", len(results))
	}

	if len(results[0]) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(results[0]))
	}
}
