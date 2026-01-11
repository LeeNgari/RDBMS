package integration_test

import (
	"testing"

	"github.com/leengari/mini-rdbms/internal/domain/data"
	"github.com/leengari/mini-rdbms/internal/query/indexing"
	"github.com/leengari/mini-rdbms/internal/query/operations/crud"
	"github.com/leengari/mini-rdbms/internal/query/operations/projection"
	"github.com/leengari/mini-rdbms/internal/query/operations/testutil"
	"github.com/leengari/mini-rdbms/internal/storage/loader"
)

// TestCRUDOperations tests all CRUD operations with real database
func TestCRUDOperations(t *testing.T) {
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

	t.Run("SelectAll", func(t *testing.T) {
		rows := crud.SelectAll(usersTable, nil)
		if len(rows) == 0 {
			t.Error("Expected rows, got none")
		}
		t.Logf("Found %d users", len(rows))
	})

	t.Run("SelectWithProjection", func(t *testing.T) {
		proj := projection.NewProjectionWithColumns(
			projection.ColumnRef{Column: "id"},
			projection.ColumnRef{Column: "username"},
		)
		rows := crud.SelectAll(usersTable, proj)
		
		if len(rows) == 0 {
			t.Error("Expected rows, got none")
		}

		// Verify only projected columns exist
		for i, row := range rows {
			testutil.AssertColumnExists(t, row, "id", "Row "+string(rune(i)))
			testutil.AssertColumnExists(t, row, "username", "Row "+string(rune(i)))
			testutil.AssertColumnNotExists(t, row, "email", "Row "+string(rune(i)))
		}
	})

	t.Run("SelectWhere", func(t *testing.T) {
		// Find users with specific username
		rows := crud.SelectWhere(usersTable, func(row data.Row) bool {
			username, ok := row["username"].(string)
			return ok && username == "bob"
		}, nil)

		if len(rows) != 1 {
			t.Errorf("Expected 1 user named bob, got %d", len(rows))
		}
	})

	t.Run("SelectByUniqueIndex", func(t *testing.T) {
		// First, get all rows to find a valid ID
		allRows := crud.SelectAll(usersTable, nil)
		if len(allRows) == 0 {
			t.Skip("No users in database to test with")
		}

		// Get the first user's ID
		firstUserID, ok := allRows[0]["id"].(int64)
		if !ok {
			t.Fatal("First user doesn't have a valid ID")
		}

		// Now test SelectByUniqueIndex with that ID
		row, found := crud.SelectByUniqueIndex(usersTable, "id", firstUserID, nil)
		if !found {
			t.Errorf("Expected to find user with id=%d", firstUserID)
		}
		if row == nil {
			t.Error("Expected non-nil row")
		}
		
		// Verify we got the right user
		if row != nil {
			if rowID, ok := row["id"].(int64); ok && rowID != firstUserID {
				t.Errorf("Expected id=%d, got id=%d", firstUserID, rowID)
			}
		}
	})

	t.Run("Update", func(t *testing.T) {
		// Update a user's email
		updated, err := crud.Update(usersTable, func(row data.Row) bool {
			id, ok := row["id"].(int64)
			return ok && id == int64(2)
		}, data.Row{
			"email": "newemail@example.com",
		})

		testutil.AssertNoError(t, err, "Update operation")
		if updated == 0 {
			t.Error("Expected to update at least 1 row")
		}

		// Verify update
		row, found := crud.SelectByUniqueIndex(usersTable, "id", int64(2), nil)
		if !found {
			t.Fatal("User not found after update")
		}
		if email, ok := row["email"].(string); !ok || email != "newemail@example.com" {
			t.Errorf("Expected email to be updated, got: %v", row["email"])
		}
	})
}

// TestProjectionFeatures tests column projection features
func TestProjectionFeatures(t *testing.T) {
	table := testutil.CreateTestTable("test")
	table.Rows = []data.Row{
		{"id": int64(1), "name": "Alice", "email": "alice@example.com", "age": int64(30)},
		{"id": int64(2), "name": "Bob", "email": "bob@example.com", "age": int64(25)},
	}

	t.Run("ProjectionWithAlias", func(t *testing.T) {
		proj := projection.NewProjectionWithColumns(
			projection.ColumnRef{Column: "id", Alias: "user_id"},
			projection.ColumnRef{Column: "name", Alias: "username"},
		)

		rows := make([]data.Row, len(table.Rows))
		for i, row := range table.Rows {
			rows[i] = projection.ProjectRow(row, proj, table.Name)
		}

		testutil.AssertRowCount(t, len(rows), 2, "Projected rows")
		testutil.AssertColumnExists(t, rows[0], "user_id", "First row")
		testutil.AssertColumnExists(t, rows[0], "username", "First row")
		testutil.AssertColumnNotExists(t, rows[0], "id", "First row")
		testutil.AssertColumnNotExists(t, rows[0], "name", "First row")
	})

	t.Run("ProjectionValidation", func(t *testing.T) {
		// Valid projection
		validProj := projection.NewProjectionWithColumns(
			projection.ColumnRef{Column: "id"},
			projection.ColumnRef{Column: "name"},
		)
		err := projection.ValidateProjection(table, validProj)
		testutil.AssertNoError(t, err, "Valid projection")

		// Invalid projection
		invalidProj := projection.NewProjectionWithColumns(
			projection.ColumnRef{Column: "nonexistent"},
		)
		err = projection.ValidateProjection(table, invalidProj)
		testutil.AssertError(t, err, "Invalid projection")
	})
}
