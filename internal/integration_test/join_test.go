package integration_test

import (
	"testing"

	"github.com/leengari/mini-rdbms/internal/domain/data"
	"github.com/leengari/mini-rdbms/internal/query/indexing"
	"github.com/leengari/mini-rdbms/internal/query/operations/join"
	"github.com/leengari/mini-rdbms/internal/query/operations/projection"
	"github.com/leengari/mini-rdbms/internal/query/operations/testutil"
	"github.com/leengari/mini-rdbms/internal/storage/loader"
)

// TestJoinOperations tests all JOIN types with real database
func TestJoinOperations(t *testing.T) {
	// Load test database
	db, err := loader.LoadDatabase("../../databases/testdb")
	if err != nil {
		t.Fatalf("Failed to load database: %v", err)
	}

	// Build indexes
	if err := indexing.BuildDatabaseIndexes(db); err != nil {
		t.Fatalf("Failed to build indexes: %v", err)
	}

	usersTable, ok := db.Tables["users"]
	if !ok {
		t.Fatal("users table not found")
	}

	ordersTable, ok := db.Tables["orders"]
	if !ok {
		t.Skip("orders table not found - skipping JOIN tests")
	}

	t.Run("InnerJoin", func(t *testing.T) {
		results, err := join.ExecuteJoin(
			usersTable, ordersTable,
			"id", "user_id",
			join.JoinTypeInner,
			nil, nil,
		)

		testutil.AssertNoError(t, err, "INNER JOIN")
		if len(results) == 0 {
			t.Error("Expected JOIN results, got none")
		}

		// Verify joined row structure
		for _, row := range results {
			if _, exists := row.Get("users.id"); !exists {
				t.Error("Expected users.id in joined row")
			}
			if _, exists := row.Get("orders.product"); !exists {
				t.Error("Expected orders.product in joined row")
			}
		}

		t.Logf("INNER JOIN returned %d rows", len(results))
	})

	t.Run("LeftJoin", func(t *testing.T) {
		results, err := join.ExecuteJoin(
			usersTable, ordersTable,
			"id", "user_id",
			join.JoinTypeLeft,
			nil, nil,
		)

		testutil.AssertNoError(t, err, "LEFT JOIN")
		
		// LEFT JOIN should return at least as many rows as INNER JOIN
		if len(results) == 0 {
			t.Error("Expected LEFT JOIN results, got none")
		}

		t.Logf("LEFT JOIN returned %d rows", len(results))
	})

	t.Run("RightJoin", func(t *testing.T) {
		results, err := join.ExecuteJoin(
			usersTable, ordersTable,
			"id", "user_id",
			join.JoinTypeRight,
			nil, nil,
		)

		testutil.AssertNoError(t, err, "RIGHT JOIN")
		if len(results) == 0 {
			t.Error("Expected RIGHT JOIN results, got none")
		}

		t.Logf("RIGHT JOIN returned %d rows", len(results))
	})

	t.Run("FullOuterJoin", func(t *testing.T) {
		results, err := join.ExecuteJoin(
			usersTable, ordersTable,
			"id", "user_id",
			join.JoinTypeFull,
			nil, nil,
		)

		testutil.AssertNoError(t, err, "FULL OUTER JOIN")
		if len(results) == 0 {
			t.Error("Expected FULL JOIN results, got none")
		}

		t.Logf("FULL OUTER JOIN returned %d rows", len(results))
	})

	t.Run("JoinWithProjection", func(t *testing.T) {
		proj := projection.NewProjectionWithColumns(
			projection.ColumnRef{Table: "users", Column: "username"},
			projection.ColumnRef{Table: "orders", Column: "product"},
			projection.ColumnRef{Table: "orders", Column: "amount"},
		)

		results, err := join.ExecuteJoin(
			usersTable, ordersTable,
			"id", "user_id",
			join.JoinTypeInner,
			nil, proj,
		)

		testutil.AssertNoError(t, err, "JOIN with projection")
		if len(results) == 0 {
			t.Error("Expected JOIN results, got none")
		}

		// Verify only projected columns exist
		for _, row := range results {
			if len(row.Data) != 3 {
				t.Errorf("Expected 3 columns, got %d", len(row.Data))
			}
			if _, exists := row.Get("users.username"); !exists {
				t.Error("Expected users.username in projected row")
			}
			if _, exists := row.Get("orders.product"); !exists {
				t.Error("Expected orders.product in projected row")
			}
			if _, exists := row.Get("orders.amount"); !exists {
				t.Error("Expected orders.amount in projected row")
			}
		}

		t.Logf("JOIN with projection returned %d rows", len(results))
	})

	t.Run("JoinWithPredicate", func(t *testing.T) {
		// Only orders with amount > 50
		predicate := func(row data.JoinedRow) bool {
			amount, exists := row.Get("orders.amount")
			if !exists {
				return false
			}
			amountVal, ok := amount.(float64)
			return ok && amountVal > 50.0
		}

		results, err := join.ExecuteJoin(
			usersTable, ordersTable,
			"id", "user_id",
			join.JoinTypeInner,
			predicate, nil,
		)

		testutil.AssertNoError(t, err, "JOIN with predicate")
		
		// Verify all results match predicate
		for _, row := range results {
			amount, _ := row.Get("orders.amount")
			if amountVal, ok := amount.(float64); ok && amountVal <= 50.0 {
				t.Errorf("Expected amount > 50, got %f", amountVal)
			}
		}

		t.Logf("JOIN with predicate returned %d rows", len(results))
	})
}

// TestJoinWithTestData tests JOIN operations with controlled test data
func TestJoinWithTestData(t *testing.T) {
	users := testutil.CreateUsersTable()
	orders := testutil.CreateOrdersTable()

	t.Run("InnerJoinBasic", func(t *testing.T) {
		results, err := join.ExecuteJoin(
			users, orders,
			"id", "user_id",
			join.JoinTypeInner,
			nil, nil,
		)

		testutil.AssertNoError(t, err, "INNER JOIN")
		testutil.AssertRowCount(t, len(results), 3, "INNER JOIN results")
	})

	t.Run("LeftJoinWithUnmatched", func(t *testing.T) {
		results, err := join.ExecuteJoin(
			users, orders,
			"id", "user_id",
			join.JoinTypeLeft,
			nil, nil,
		)

		testutil.AssertNoError(t, err, "LEFT JOIN")
		// Should have 4 rows: 3 matched + 1 unmatched (charlie)
		testutil.AssertRowCount(t, len(results), 4, "LEFT JOIN results")

		// Find charlie's row (should have NULL order columns)
		charlieFound := false
		for _, row := range results {
			username, _ := row.Get("users.username")
			if username == "charlie" {
				charlieFound = true
				product, _ := row.Get("orders.product")
				testutil.AssertNullValue(t, product, "Charlie's order product")
			}
		}

		if !charlieFound {
			t.Error("Expected to find charlie in LEFT JOIN results")
		}
	})

	t.Run("RightJoinWithOrphans", func(t *testing.T) {
		// Create fresh tables
		testUsers := testutil.CreateUsersTable()
		testOrders := testutil.CreateOrdersTable()
		
		// Add orphaned order (no matching user)
		orphanOrder := data.Row{
			"id":      int64(99),
			"user_id": int64(999), // No such user
			"product": "Orphan Product",
			"amount":  50.0,
		}
		testOrders.Rows = append(testOrders.Rows, orphanOrder)
		
		results, err := join.ExecuteJoin(
			testUsers, testOrders,
			"id", "user_id",
			join.JoinTypeRight,
			nil, nil,
		)
		
		testutil.AssertNoError(t, err, "RIGHT JOIN")
		// Should have 4 rows: 3 matched + 1 orphaned
		testutil.AssertRowCount(t, len(results), 4, "RIGHT JOIN results")
		
		// Find orphaned order (should have NULL user columns)
		orphanFound := false
		for _, row := range results {
			product, _ := row.Get("orders.product")
			if product == "Orphan Product" {
				orphanFound = true
				username, _ := row.Get("users.username")
				testutil.AssertNullValue(t, username, "Orphan order username")
			}
		}
		
		if !orphanFound {
			t.Error("Expected to find orphaned order in RIGHT JOIN results")
		}
	})

	t.Run("FullJoinWithBothUnmatched", func(t *testing.T) {
		// Create fresh tables
		testUsers := testutil.CreateUsersTable()
		testOrders := testutil.CreateOrdersTable()
		
		// Add orphaned order
		orphanOrder := data.Row{
			"id":      int64(99),
			"user_id": int64(999),
			"product": "Orphan Product",
			"amount":  50.0,
		}
		testOrders.Rows = append(testOrders.Rows, orphanOrder)
		
		results, err := join.ExecuteJoin(
			testUsers, testOrders,
			"id", "user_id",
			join.JoinTypeFull,
			nil, nil,
		)
		
		testutil.AssertNoError(t, err, "FULL JOIN")
		// Should have 5 rows: 3 matched + 1 unmatched user + 1 orphaned order
		testutil.AssertRowCount(t, len(results), 5, "FULL JOIN results")
		
		// Verify both unmatched user and orphaned order exist
		charlieFound := false
		orphanFound := false
		
		for _, row := range results {
			username, _ := row.Get("users.username")
			product, _ := row.Get("orders.product")
			
			if username == "charlie" {
				charlieFound = true
				testutil.AssertNullValue(t, product, "Charlie's product")
			}
			
			if product == "Orphan Product" {
				orphanFound = true
				testutil.AssertNullValue(t, username, "Orphan username")
			}
		}
		
		if !charlieFound {
			t.Error("Expected to find charlie (unmatched user)")
		}
		if !orphanFound {
			t.Error("Expected to find orphaned order")
		}
	})

	t.Run("JoinWithPredicate", func(t *testing.T) {
		// Predicate: only orders with amount > 50
		predicate := func(row data.JoinedRow) bool {
			amount, exists := row.Get("orders.amount")
			if !exists {
				return false
			}
			amountVal, ok := amount.(float64)
			return ok && amountVal > 50.0
		}
		
		results, err := join.ExecuteJoin(
			users, orders,
			"id", "user_id",
			join.JoinTypeInner,
			predicate, nil,
		)
		
		testutil.AssertNoError(t, err, "JOIN with predicate")
		// Only 2 orders have amount > 50 (Laptop: 999.99, Keyboard: 75.00)
		testutil.AssertRowCount(t, len(results), 2, "Filtered JOIN results")
		
		// Verify all results match predicate
		for _, row := range results {
			amount, _ := row.Get("orders.amount")
			if amountVal, ok := amount.(float64); ok {
				if amountVal <= 50.0 {
					t.Errorf("Expected amount > 50, got %f", amountVal)
				}
			}
		}
	})

	t.Run("JoinWithProjection", func(t *testing.T) {
		proj := projection.NewProjectionWithColumns(
			projection.ColumnRef{Table: "users", Column: "username"},
			projection.ColumnRef{Table: "orders", Column: "product"},
		)
		
		results, err := join.ExecuteJoin(
			users, orders,
			"id", "user_id",
			join.JoinTypeInner,
			nil, proj,
		)
		
		testutil.AssertNoError(t, err, "JOIN with projection")
		testutil.AssertRowCount(t, len(results), 3, "Projected JOIN results")
		
		// Verify only projected columns exist
		for _, row := range results {
			testutil.AssertColumnCount(t, len(row.Data), 2, "Projected row")
			testutil.AssertJoinedColumnExists(t, row.Data, "users.username", "Projected row")
			testutil.AssertJoinedColumnExists(t, row.Data, "orders.product", "Projected row")
			
			// Verify other columns don't exist
			if _, exists := row.Get("users.id"); exists {
				t.Error("users.id should not exist in projected row")
			}
			if _, exists := row.Get("orders.amount"); exists {
				t.Error("orders.amount should not exist in projected row")
			}
		}
	})

	t.Run("JoinEmptyTables", func(t *testing.T) {
		// Create empty tables with proper schemas
		emptyUsers := testutil.CreateUsersTable()
		emptyUsers.Rows = []data.Row{} // Clear rows
		
		emptyOrders := testutil.CreateOrdersTable()
		emptyOrders.Rows = []data.Row{} // Clear rows
		
		results, err := join.ExecuteJoin(
			emptyUsers, emptyOrders,
			"id", "user_id",
			join.JoinTypeInner,
			nil, nil,
		)
		
		testutil.AssertNoError(t, err, "JOIN with empty tables")
		testutil.AssertRowCount(t, len(results), 0, "Empty JOIN results")
	})

	t.Run("LeftJoinEmptyRight", func(t *testing.T) {
		testUsers := testutil.CreateUsersTable()
		
		// Create empty orders table with proper schema
		emptyOrders := testutil.CreateOrdersTable()
		emptyOrders.Rows = []data.Row{} // Clear rows
		
		results, err := join.ExecuteJoin(
			testUsers, emptyOrders,
			"id", "user_id",
			join.JoinTypeLeft,
			nil, nil,
		)
		
		testutil.AssertNoError(t, err, "LEFT JOIN with empty right table")
		// Should return all users with NULL order columns
		testutil.AssertRowCount(t, len(results), 3, "LEFT JOIN results")
		
		// All rows should have NULL order columns
		for _, row := range results {
			product, _ := row.Get("orders.product")
			testutil.AssertNullValue(t, product, "Product in empty right JOIN")
		}
	})
}
