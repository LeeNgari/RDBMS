package storage

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/leengari/mini-rdbms/internal/engine"
)

// LoadDatabase loads the database from the given directory path
func LoadDatabase(dbPath string, logger *slog.Logger) (*engine.Database, error) {
	metaPath := filepath.Join(dbPath, "meta.json")

	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read database meta: %w", err)
	}

	var meta DatabaseMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to parse database meta: %w", err)
	}

	db := &engine.Database{
		Name:   meta.Name,
		Path:   dbPath,
		Tables: make(map[string]*engine.Table),
	}

	// Read all entries in the database directory
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read database directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		tableName := entry.Name()
		tablePath := filepath.Join(dbPath, tableName)

		table, err := LoadTable(tablePath, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to load table %s: %w", tableName, err)
		}

		db.Tables[table.Name] = table
	}

	logger.Info("Database loaded successfully",
		slog.String("name", db.Name),
		slog.String("path", dbPath),
		slog.Int("table_count", len(db.Tables)),
	)		

	return db, nil
}

