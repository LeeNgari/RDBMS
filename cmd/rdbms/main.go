package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/leengari/mini-rdbms/internal/infrastructure/logging"
	"github.com/leengari/mini-rdbms/internal/query/indexing"
	"github.com/leengari/mini-rdbms/internal/storage/loader"
	"github.com/leengari/mini-rdbms/internal/storage/writer"
)

func main() {
	logger, closeFn := logging.SetupLogger()
	defer closeFn()

	slog.SetDefault(logger)
	time.Sleep(1 * time.Second)
	slog.Info("Starting RDBMS application...")

	// Load Database
	db, err := loader.LoadDatabase("databases/testdb")
	if err != nil {
		slog.Error("failed to load database", "error", err)
		closeFn()
		os.Exit(1)
	}

	// Save database on shutdown
	defer func() {
		slog.Info("Shutting down - saving database...")
		if err := writer.SaveDatabase(db); err != nil {
			slog.Error("shutdown save failed", "error", err)
		}
	}()

	// Build Indexes
	if err := indexing.BuildDatabaseIndexes(db); err != nil {
		slog.Error("Index building failed", "error", err)
		closeFn()
		os.Exit(1)
	}

	// Get users table
	usersTable, ok := db.Tables["users"]
	if !ok {
		slog.Error("table 'users' not found")
		closeFn()
		os.Exit(1)
	}

	// Run CRUD operation tests
	testCRUDOperations(usersTable)

	// Check if orders table exists for JOIN tests
	if ordersTable, ok := db.Tables["orders"]; ok {
		testJoinOperations(usersTable, ordersTable)
	} else {
		slog.Warn("orders table not found - skipping JOIN tests")
		slog.Info("To test JOINs, create databases/testdb/orders/ with meta.json and data.json")
	}

	slog.Info("Application ready - all operations tested!")
}
