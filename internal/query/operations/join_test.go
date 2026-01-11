package operations_test

import (
	"testing"

	"github.com/leengari/mini-rdbms/internal/domain/data"
	"github.com/leengari/mini-rdbms/internal/domain/schema"
	"github.com/leengari/mini-rdbms/internal/query/indexing"
	"github.com/leengari/mini-rdbms/internal/query/operations"
)

// Helper to create users table for JOIN tests
func createUsersTable() *schema.Table {
	table := &schema.Table{
		Name: "users",
		Schema: &schema.TableSchema{
			TableName: "users",
			Columns: []schema.Column{
				{Name: "id", Type: schema.ColumnTypeInt, PrimaryKey: true, NotNull: true},
				{Name: "username", Type: schema.ColumnTypeText, NotNull: true},
				{Name: "email", Type: schema.ColumnTypeText},
			},
		},
		Rows: []data.Row{
			{"id": int64(1), "username": "alice", "email": "alice@example.com"},
			{"id": int64(2), "username": "bob", "email": "bob@example.com"},
			{"id": int64(3), "username": "charlie", "email": "charlie@example.com"},
		},
		Indexes: make(map[string]*data.Index),
	}
	indexing.BuildIndexes(table)
	return table
}

// Helper to create orders table for JOIN tests
func createOrdersTable() *schema.Table {
	table := &schema.Table{
		Name: "orders",
		Schema: &schema.TableSchema{
			TableName: "orders",
			Columns: []schema.Column{
				{Name: "id", Type: schema.ColumnTypeInt, PrimaryKey: true, NotNull: true},
				{Name: "user_id", Type: schema.ColumnTypeInt, NotNull: true},
				{Name: "product", Type: schema.ColumnTypeText, NotNull: true},
				{Name: "amount", Type: schema.ColumnTypeFloat},
			},
		},
		Rows: []data.Row{
			{"id": int64(1), "user_id": int64(1), "product": "Laptop", "amount": 999.99},
			{"id": int64(2), "user_id": int64(1), "product": "Mouse", "amount": 25.50},
			{"id": int64(3), "user_id": int64(2), "product": "Keyboard", "amount": 75.00},
			// Note: user_id 3 (charlie) has no orders
		},
		Indexes: make(map[string]*data.Index),
	}
	indexing.BuildIndexes(table)
	return table
}

// TestInnerJoin_Basic tests basic INNER JOIN functionality
func TestInnerJoin_Basic(t *testing.T) {
	users := createUsersTable()
	orders := createOrdersTable()

	results, err := operations.ExecuteJoin(
		users, orders,
		"id", "user_id",
		operations.JoinTypeInner,
		nil, nil,
	)

	if err != nil {
		t.Fatalf("INNER JOIN failed: %v", err)
	}

	// Should have 3 results (alice has 2 orders, bob has 1)
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Check that all results have both user and order data
	for i, row := range results {
		if _, exists := row.Get("users.id"); !exists {
			t.Errorf("Row %d missing users.id", i)
		}
		if _, exists := row.Get("orders.product"); !exists {
			t.Errorf("Row %d missing orders.product", i)
		}
	}
}

// TestInnerJoin_WithPredicate tests INNER JOIN with filtering
func TestInnerJoin_WithPredicate(t *testing.T) {
	users := createUsersTable()
	orders := createOrdersTable()

	// Only orders with amount > 50
	predicate := func(row data.JoinedRow) bool {
		amount, exists := row.Get("orders.amount")
		if !exists {
			return false
		}
		amountVal, ok := amount.(float64)
		return ok && amountVal > 50.0
	}

	results, err := operations.ExecuteJoin(
		users, orders,
		"id", "user_id",
		operations.JoinTypeInner,
		predicate, nil,
	)

	if err != nil {
		t.Fatalf("INNER JOIN with predicate failed: %v", err)
	}

	// Should have 2 results (Laptop: 999.99, Keyboard: 75.00)
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

// TestLeftJoin_AllMatches tests LEFT JOIN when all left rows have matches
func TestLeftJoin_AllMatches(t *testing.T) {
	users := createUsersTable()
	orders := createOrdersTable()

	// Remove charlie (who has no orders) for this test
	users.Rows = users.Rows[:2]

	results, err := operations.ExecuteJoin(
		users, orders,
		"id", "user_id",
		operations.JoinTypeLeft,
		nil, nil,
	)

	if err != nil {
		t.Fatalf("LEFT JOIN failed: %v", err)
	}

	// Should have 3 results (same as INNER JOIN in this case)
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

// TestLeftJoin_WithUnmatched tests LEFT JOIN with unmatched left rows
func TestLeftJoin_WithUnmatched(t *testing.T) {
	users := createUsersTable()
	orders := createOrdersTable()

	results, err := operations.ExecuteJoin(
		users, orders,
		"id", "user_id",
		operations.JoinTypeLeft,
		nil, nil,
	)

	if err != nil {
		t.Fatalf("LEFT JOIN failed: %v", err)
	}

	// Should have 4 results (3 orders + 1 user with no orders)
	if len(results) != 4 {
		t.Errorf("Expected 4 results, got %d", len(results))
	}

	// Find charlie's row (should have NULL order columns)
	charlieFound := false
	for _, row := range results {
		username, _ := row.Get("users.username")
		if username == "charlie" {
			charlieFound = true
			// Check that order columns are NULL
			product, _ := row.Get("orders.product")
			if product != nil {
				t.Error("Expected NULL for orders.product for charlie")
			}
		}
	}

	if !charlieFound {
		t.Error("Charlie not found in LEFT JOIN results")
	}
}

// TestRightJoin_Basic tests RIGHT JOIN functionality
func TestRightJoin_Basic(t *testing.T) {
	users := createUsersTable()
	orders := createOrdersTable()

	results, err := operations.ExecuteJoin(
		users, orders,
		"id", "user_id",
		operations.JoinTypeRight,
		nil, nil,
	)

	if err != nil {
		t.Fatalf("RIGHT JOIN failed: %v", err)
	}

	// Should have 3 results (all orders have matching users)
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

// TestRightJoin_WithOrphanedRows tests RIGHT JOIN with orphaned right rows
func TestRightJoin_WithOrphanedRows(t *testing.T) {
	users := createUsersTable()
	orders := createOrdersTable()

	// Add an orphaned order (user_id 999 doesn't exist)
	orders.Rows = append(orders.Rows, data.Row{
		"id":      int64(4),
		"user_id": int64(999),
		"product": "Monitor",
		"amount":  299.99,
	})

	results, err := operations.ExecuteJoin(
		users, orders,
		"id", "user_id",
		operations.JoinTypeRight,
		nil, nil,
	)

	if err != nil {
		t.Fatalf("RIGHT JOIN failed: %v", err)
	}

	// Should have 4 results (3 matched + 1 orphaned)
	if len(results) != 4 {
		t.Errorf("Expected 4 results, got %d", len(results))
	}

	// Find the orphaned order
	orphanFound := false
	for _, row := range results {
		product, _ := row.Get("orders.product")
		if product == "Monitor" {
			orphanFound = true
			// Check that user columns are NULL
			username, _ := row.Get("users.username")
			if username != nil {
				t.Error("Expected NULL for users.username for orphaned order")
			}
		}
	}

	if !orphanFound {
		t.Error("Orphaned order not found in RIGHT JOIN results")
	}
}

// TestFullJoin_Basic tests FULL OUTER JOIN functionality
func TestFullJoin_Basic(t *testing.T) {
	users := createUsersTable()
	orders := createOrdersTable()

	results, err := operations.ExecuteJoin(
		users, orders,
		"id", "user_id",
		operations.JoinTypeFull,
		nil, nil,
	)

	if err != nil {
		t.Fatalf("FULL JOIN failed: %v", err)
	}

	// Should have 4 results (3 orders + 1 user with no orders)
	if len(results) != 4 {
		t.Errorf("Expected 4 results, got %d", len(results))
	}
}

// TestFullJoin_WithBothUnmatched tests FULL JOIN with unmatched rows on both sides
func TestFullJoin_WithBothUnmatched(t *testing.T) {
	users := createUsersTable()
	orders := createOrdersTable()

	// Add an orphaned order
	orders.Rows = append(orders.Rows, data.Row{
		"id":      int64(4),
		"user_id": int64(999),
		"product": "Monitor",
		"amount":  299.99,
	})

	results, err := operations.ExecuteJoin(
		users, orders,
		"id", "user_id",
		operations.JoinTypeFull,
		nil, nil,
	)

	if err != nil {
		t.Fatalf("FULL JOIN failed: %v", err)
	}

	// Should have 5 results (3 matched + 1 unmatched user + 1 orphaned order)
	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	// Count NULL rows
	unmatchedUsers := 0
	unmatchedOrders := 0

	for _, row := range results {
		username, _ := row.Get("users.username")
		product, _ := row.Get("orders.product")

		if username == nil && product != nil {
			unmatchedOrders++
		}
		if username != nil && product == nil {
			unmatchedUsers++
		}
	}

	if unmatchedUsers != 1 {
		t.Errorf("Expected 1 unmatched user, got %d", unmatchedUsers)
	}
	if unmatchedOrders != 1 {
		t.Errorf("Expected 1 unmatched order, got %d", unmatchedOrders)
	}
}

// TestJoin_WithProjection tests JOIN with column projection
func TestJoin_WithProjection(t *testing.T) {
	users := createUsersTable()
	orders := createOrdersTable()

	// SELECT users.username, orders.product
	proj := operations.NewProjectionWithColumns(
		operations.ColumnRef{Table: "users", Column: "username"},
		operations.ColumnRef{Table: "orders", Column: "product"},
	)

	results, err := operations.ExecuteJoin(
		users, orders,
		"id", "user_id",
		operations.JoinTypeInner,
		nil, proj,
	)

	if err != nil {
		t.Fatalf("JOIN with projection failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Check that only requested columns are present
	for i, row := range results {
		if len(row.Data) != 2 {
			t.Errorf("Row %d: expected 2 columns, got %d", i, len(row.Data))
		}

		if _, exists := row.Get("users.username"); !exists {
			t.Errorf("Row %d: missing users.username", i)
		}
		if _, exists := row.Get("orders.product"); !exists {
			t.Errorf("Row %d: missing orders.product", i)
		}
		if _, exists := row.Get("users.id"); exists {
			t.Errorf("Row %d: should not have users.id", i)
		}
	}
}

// TestJoin_TypeMismatch tests JOIN with incompatible column types
func TestJoin_TypeMismatch(t *testing.T) {
	users := createUsersTable()
	orders := createOrdersTable()

	// Try to join on incompatible types (id is INT, product is TEXT)
	_, err := operations.ExecuteJoin(
		users, orders,
		"id", "product",
		operations.JoinTypeInner,
		nil, nil,
	)

	if err == nil {
		t.Error("Expected error for type mismatch, got nil")
	}
}

// TestJoin_NonExistentColumn tests JOIN with non-existent column
func TestJoin_NonExistentColumn(t *testing.T) {
	users := createUsersTable()
	orders := createOrdersTable()

	// Try to join on non-existent column
	_, err := operations.ExecuteJoin(
		users, orders,
		"nonexistent", "user_id",
		operations.JoinTypeInner,
		nil, nil,
	)

	if err == nil {
		t.Error("Expected error for non-existent column, got nil")
	}
}
