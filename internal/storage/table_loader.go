package storage

import (
	"encoding/json"
	"os"
	"log/slog"
	"path/filepath"

	"github.com/leengari/mini-rdbms/internal/engine"
)

func LoadTable(path string, logger *slog.Logger) (*engine.Table, error) {
	metaPath := filepath.Join(path, "meta.json")
	dataPath := filepath.Join(path, "data.json")

	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, err
	}

	var meta TableMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, err
	}

	schema := &engine.TableSchema{
		TableName: meta.Name,
		Columns:   make([]engine.Column, 0),
	}

	for _, c := range meta.Columns {
		col := engine.Column{
			Name:          c.Name,
			Type:          engine.ColumnType(c.Type),
			PrimaryKey:    c.PrimaryKey,
			Unique:        c.Unique,
			NotNull:       c.NotNull,
			AutoIncrement: c.AutoIncrement,
		}
		schema.Columns = append(schema.Columns, col)
	}

	rows := []engine.Row{}
	if _, err := os.Stat(dataPath); err == nil {
		dataBytes, err := os.ReadFile(dataPath)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(dataBytes, &rows); err != nil {
			return nil, err
		}
	}


	table := &engine.Table{
		Name:        meta.Name,
		Path:        path,
		Schema:      schema,
		Rows:        rows,
		Indexes:     make(map[string]*engine.Index),
		LastInsertID: meta.LastInsertID,
	}
	logger.Info("table loaded",
		slog.String("table", table.Name),
		slog.Int("rows", len(rows)),
	)

	return table, nil
}
