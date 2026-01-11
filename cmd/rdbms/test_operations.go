package main

import (
	"log/slog"

	"github.com/leengari/mini-rdbms/internal/domain/data"
	"github.com/leengari/mini-rdbms/internal/domain/schema"
	"github.com/leengari/mini-rdbms/internal/query/operations"
)

// testCRUDOperations demonstrates all CRUD operations
func testCRUDOperations(usersTable *schema.Table) {
	slog.Info("=== Testing CRUD Operations ===")

	// SELECT All (with nil projection = all columns)
	allRows := operations.SelectAll(usersTable, nil)
	slog.Info("Initial row count", "count", len(allRows))

	// Demonstrate column projection - SELECT id, username
	slog.Info("=== Testing Column Projection ===")
	proj := operations.NewProjectionWithColumns(
		operations.ColumnRef{Column: "id"},
		operations.ColumnRef{Column: "username"},
	)
	projectedRows := operations.SelectAll(usersTable, proj)
	slog.Info("Projected rows (id, username only)", "count", len(projectedRows))
	for _, row := range projectedRows {
		slog.Info("Projected row", "data", row)
	}

	// UPDATE
	slog.Info("=== Testing UPDATE ===")
	updated, err := operations.Update(usersTable, func(r data.Row) bool {
		return r["username"] == "alice"
	}, data.Row{
		"email":     "alice.updated@example.com",
		"is_active": false,
	})
	if err != nil {
		slog.Error("UPDATE failed", "error", err)
	} else {
		slog.Info("UPDATE successful", "rows_updated", updated)
		if row, found := operations.SelectByUniqueIndex(usersTable, "username", "alice", nil); found {
			slog.Info("Verified alice's updated data",
				"email", row["email"],
				"is_active", row["is_active"],
			)
		}
	}

	// UPDATE by ID
	slog.Info("=== Testing UPDATE by ID ===")
	err = operations.UpdateByID(usersTable, int64(2), data.Row{
		"email": "bob.new@example.com",
	})
	if err != nil {
		slog.Error("UPDATE by ID failed", "error", err)
	} else {
		slog.Info("UPDATE by ID successful")
		if row, found := operations.SelectByUniqueIndex(usersTable, "id", int64(2), nil); found {
			slog.Info("Verified bob's updated email", "email", row["email"])
		}
	}

	// DELETE
	slog.Info("=== Testing DELETE ===")
	deleted, err := operations.Delete(usersTable, func(r data.Row) bool {
		isActive, ok := r["is_active"].(bool)
		return ok && !isActive
	})
	if err != nil {
		slog.Error("DELETE failed", "error", err)
	} else {
		slog.Info("DELETE successful", "rows_deleted", deleted)
	}

	// DELETE by ID
	slog.Info("=== Testing DELETE by ID ===")
	err = operations.DeleteByID(usersTable, int64(3))
	if err != nil {
		slog.Error("DELETE by ID failed", "error", err)
	} else {
		slog.Info("DELETE by ID successful (charlie deleted)")
	}

	// Final SELECT
	finalRows := operations.SelectAll(usersTable, nil)
	slog.Info("Final row count after operations", "count", len(finalRows))

	for _, row := range finalRows {
		slog.Info("Remaining user",
			"id", row["id"],
			"username", row["username"],
			"email", row["email"],
			"is_active", row["is_active"],
		)
	}
}

// testJoinOperations demonstrates all JOIN types
func testJoinOperations(usersTable, ordersTable *schema.Table) {
	slog.Info("=== Testing JOIN Operations ===")

	// INNER JOIN - only matching rows
	slog.Info("=== Testing INNER JOIN ===")
	innerResults, err := operations.ExecuteJoin(
		usersTable,
		ordersTable,
		"id",
		"user_id",
		operations.JoinTypeInner,
		nil,
		nil,
	)
	if err != nil {
		slog.Error("INNER JOIN failed", "error", err)
	} else {
		slog.Info("INNER JOIN successful", "result_rows", len(innerResults))
		for i, row := range innerResults {
			slog.Info("INNER JOIN row",
				"index", i,
				"user_id", row.Data["users.id"],
				"username", row.Data["users.username"],
				"product", row.Data["orders.product"],
			)
		}
	}

	// LEFT JOIN - all users, even those without orders
	slog.Info("=== Testing LEFT JOIN ===")
	leftResults, err := operations.ExecuteJoin(
		usersTable,
		ordersTable,
		"id",
		"user_id",
		operations.JoinTypeLeft,
		nil,
		nil,
	)
	if err != nil {
		slog.Error("LEFT JOIN failed", "error", err)
	} else {
		slog.Info("LEFT JOIN successful", "result_rows", len(leftResults))
		for i, row := range leftResults {
			product, _ := row.Get("orders.product")
			slog.Info("LEFT JOIN row",
				"index", i,
				"username", row.Data["users.username"],
				"product", product, // May be NULL
			)
		}
	}

	// RIGHT JOIN - all orders, even orphaned ones
	slog.Info("=== Testing RIGHT JOIN ===")
	rightResults, err := operations.ExecuteJoin(
		usersTable,
		ordersTable,
		"id",
		"user_id",
		operations.JoinTypeRight,
		nil,
		nil,
	)
	if err != nil {
		slog.Error("RIGHT JOIN failed", "error", err)
	} else {
		slog.Info("RIGHT JOIN successful", "result_rows", len(rightResults))
		for i, row := range rightResults {
			username, _ := row.Get("users.username")
			slog.Info("RIGHT JOIN row",
				"index", i,
				"username", username, // May be NULL
				"product", row.Data["orders.product"],
			)
		}
	}

	// FULL OUTER JOIN - all users and all orders
	slog.Info("=== Testing FULL OUTER JOIN ===")
	fullResults, err := operations.ExecuteJoin(
		usersTable,
		ordersTable,
		"id",
		"user_id",
		operations.JoinTypeFull,
		nil,
		nil,
	)
	if err != nil {
		slog.Error("FULL JOIN failed", "error", err)
	} else {
		slog.Info("FULL JOIN successful", "result_rows", len(fullResults))
		for i, row := range fullResults {
			username, _ := row.Get("users.username")
			product, _ := row.Get("orders.product")
			slog.Info("FULL JOIN row",
				"index", i,
				"username", username, // May be NULL
				"product", product,   // May be NULL
			)
		}
	}

	// JOIN with column projection
	slog.Info("=== Testing JOIN with Projection ===")
	proj := operations.NewProjectionWithColumns(
		operations.ColumnRef{Table: "users", Column: "username"},
		operations.ColumnRef{Table: "orders", Column: "product"},
		operations.ColumnRef{Table: "orders", Column: "amount"},
	)
	projResults, err := operations.ExecuteJoin(
		usersTable,
		ordersTable,
		"id",
		"user_id",
		operations.JoinTypeInner,
		nil,
		proj,
	)
	if err != nil {
		slog.Error("JOIN with projection failed", "error", err)
	} else {
		slog.Info("JOIN with projection successful", "result_rows", len(projResults))
		for i, row := range projResults {
			slog.Info("Projected JOIN row",
				"index", i,
				"data", row.Data, // Only username, product, amount
			)
		}
	}

	// JOIN with predicate - expensive orders only
	slog.Info("=== Testing JOIN with Predicate ===")
	predResults, err := operations.ExecuteJoin(
		usersTable,
		ordersTable,
		"id",
		"user_id",
		operations.JoinTypeInner,
		func(row data.JoinedRow) bool {
			amount, ok := row.Data["orders.amount"].(float64)
			return ok && amount > 50.0
		},
		nil,
	)
	if err != nil {
		slog.Error("JOIN with predicate failed", "error", err)
	} else {
		slog.Info("JOIN with predicate successful", "result_rows", len(predResults))
		for i, row := range predResults {
			slog.Info("Filtered JOIN row",
				"index", i,
				"username", row.Data["users.username"],
				"product", row.Data["orders.product"],
				"amount", row.Data["orders.amount"],
			)
		}
	}
}
