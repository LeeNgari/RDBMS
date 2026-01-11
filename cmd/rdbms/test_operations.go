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

	// SELECT All
	allRows := operations.SelectAll(usersTable)
	slog.Info("Initial row count", "count", len(allRows))

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
		if row, found := operations.SelectByUniqueIndex(usersTable, "username", "alice"); found {
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
		if row, found := operations.SelectByUniqueIndex(usersTable, "id", int64(2)); found {
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
	finalRows := operations.SelectAll(usersTable)
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

// testJoinOperations demonstrates JOIN operations
func testJoinOperations(usersTable, ordersTable *schema.Table) {
	slog.Info("=== Testing JOIN Operations ===")

	// Basic INNER JOIN
	slog.Info("=== Testing Basic INNER JOIN ===")
	results, err := operations.Join(
		usersTable,
		ordersTable,
		"id",      // users.id
		"user_id", // orders.user_id
		nil,       // No predicate
	)
	if err != nil {
		slog.Error("JOIN failed", "error", err)
	} else {
		slog.Info("JOIN successful", "result_rows", len(results))
		for i, row := range results {
			slog.Info("Joined row",
				"index", i,
				"user_id", row.Data["users.id"],
				"username", row.Data["users.username"],
				"order_id", row.Data["orders.id"],
				"product", row.Data["orders.product"],
			)
		}
	}

	// JOIN with predicate
	slog.Info("=== Testing JOIN with Predicate ===")
	results, err = operations.JoinWhere(
		usersTable,
		ordersTable,
		"id",
		"user_id",
		func(row data.JoinedRow) bool {
			// Only include expensive orders (> $50)
			amount, ok := row.Data["orders.amount"].(float64)
			return ok && amount > 50.0
		},
	)
	if err != nil {
		slog.Error("JOIN with predicate failed", "error", err)
	} else {
		slog.Info("JOIN with predicate successful", "result_rows", len(results))
		for i, row := range results {
			slog.Info("Expensive order",
				"index", i,
				"username", row.Data["users.username"],
				"product", row.Data["orders.product"],
				"amount", row.Data["orders.amount"],
			)
		}
	}
}
