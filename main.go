package main

import (
	"os"

	"github.com/leengari/mini-rdbms/internal/engine"
	"github.com/leengari/mini-rdbms/internal/logging"
	"github.com/leengari/mini-rdbms/internal/storage"
)

func main() {
	logger, closeFn := logging.SetupLogger()
	defer closeFn()

	logger.Info("Starting application...")

	// 1. Load Database
	db, err := storage.LoadDatabase("databases/testdb", logger)
	if err != nil {
		logger.Error("failed to load database", "error", err)
		closeFn()
		os.Exit(1)
	}

	// 2. Build Indexes (after loading rows)
	if err := engine.BuildDatabaseIndexes(db, logger); err != nil {
		logger.Error("Index building failed", "error", err)
		closeFn()
		os.Exit(1)
	}

	// 3. Get users table
	usersTable, ok := db.Tables["users"]
	if !ok {
		logger.Error("table 'users' not found")
		closeFn()
		os.Exit(1)
	}

	// 4. Insert new valid rows (using auto-increment + required fields)
	// Note: do NOT provide "id" if it's auto-increment!
	newUsers := []engine.Row{
		{
			"username":  "frank",
			"email":     "frank@newuser.com",
			"is_active": true,
		},
		{
			"username":  "grace",
			"email":     "grace@secure.mail",
			"is_active": false,
		},
	}

	for i, row := range newUsers {
		err := usersTable.Insert(row)
		if err != nil {
			logger.Error("failed to insert new user",
				"index", i+1,
				"username", row["username"],
				"error", err,
			)
			closeFn()
			os.Exit(1)
		}
		insertedRow := usersTable.Rows[len(usersTable.Rows)-1]

		logger.Info("successfully inserted user",
			"username", insertedRow["username"],
			"email", insertedRow["email"],
			"new_id", insertedRow["id"], // should be auto-generated (6, 7, ...)
		)
	}

	// 5. Select All (after inserts)
	allRows := engine.SelectAll(usersTable)
	logger.Info("all rows after insert",
		"count", len(allRows),
		"rows", allRows,
	)

	// 6. Select with Predicate (example)
	graceUser := engine.SelectWhere(usersTable, func(r engine.Row) bool {
		return r["username"] == "grace"
	})
	logger.Info("found grace", "results", graceUser)

	// 7. Select by Unique Index (using the new auto-generated IDs)
	if row, found := engine.SelectByUniqueIndex(usersTable, "id", 6); found {
		logger.Info("found user by id=6 (first new insert)", "data", row)
	} else {
		logger.Warn("user id=6 not found")
	}

	logger.Info("Application ready")
}
