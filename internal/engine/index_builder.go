package engine

import (
	"fmt"
	"log/slog"

)

// BuildIndexes rebuilds all indexes for primary/unique columns
// Returns error on constraint violation or data inconsistency
func BuildIndexes(table *Table, logger *slog.Logger) error {

	// Clear existing indexes
	table.Indexes = make(map[string]*Index)

	for _, col := range table.Schema.Columns {
		if !col.PrimaryKey && !col.Unique {
			continue
		}

		idx := &Index{
			Column: col.Name,
			Data:   make(map[interface{}][]int),
			Unique: col.PrimaryKey || col.Unique,
		}

		for rowPos, row := range table.Rows {
			val, ok := row[col.Name]
			if !ok {
				if col.NotNull {
					return NewNotNullViolation(table.Name, col.Name, rowPos)
				}
				continue
			}

			// Optional: normalize numeric keys for auto-increment
			if col.AutoIncrement && col.PrimaryKey {
				switch v := val.(type) {
				case float64:
					val = int64(v) // JSON numbers come as float64
				case int64, int:
					// already good
				default:
					return fmt.Errorf("invalid auto-increment value in %s row %d: %v (want integer)",
						col.Name, rowPos, val)
				}
			}

			// Check type consistency (very useful during development)
			if len(idx.Data) > 0 {
				for existing := range idx.Data {
					if fmt.Sprintf("%T", existing) != fmt.Sprintf("%T", val) {
						logger.Warn("type inconsistency in column",
							slog.String("column", col.Name),
							slog.Any("previous_type", fmt.Sprintf("%T", existing)),
							slog.Any("new_type", fmt.Sprintf("%T", val)),
							slog.Int("row", rowPos))
					}
					break // only check once
				}
			}

			idx.Data[val] = append(idx.Data[val], rowPos)

			if idx.Unique && len(idx.Data[val]) > 1 {
				return NewUniqueViolation(
                    table.Name,
                    col.Name,
                    val,
                    idx.Data[val], 
                )
			}
		}

		table.Indexes[col.Name] = idx

		if logger != nil {
			logger.Debug("index built",
				slog.String("table", table.Name),
				slog.String("column", col.Name),
				slog.Int("unique_values", len(idx.Data)),
				slog.Bool("unique_constraint", idx.Unique))
		}
	}

	return nil
}

// BuildDatabaseIndexes rebuilds indexes for all tables
func BuildDatabaseIndexes(db *Database, logger *slog.Logger) error {
	for name, table := range db.Tables {
		if err := BuildIndexes(table, logger); err != nil {
			return fmt.Errorf("failed to build indexes for table %s: %w", name, err)
		}
	}
	return nil
}