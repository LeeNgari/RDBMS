package engine

import (
	"fmt"
	"regexp"
	"time"
)

// Email validation regex - reasonable balance between strictness and practicality
var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)

// validateRow checks if the given row matches the table's schema
// - Checks required fields (NOT NULL)
// - Validates type compatibility (with JSON reality)
// - Returns ConstraintError for better error handling
func (t *Table) validateRow(row Row, rowIndex int) error {
	for _, col := range t.Schema.Columns {
		val, exists := row[col.Name]

		//Handle missing value
		if !exists {
			if col.NotNull {
				return &ConstraintError{
					Table:      t.Name,
					Column:     col.Name,
					Constraint: "not_null",
					Reason:     "missing required value",
					RowIndex:   rowIndex,
				}
			}
			continue // nullable
		}

	
		//Type validation
		switch col.Type {
		case ColumnTypeText:
			if _, ok := val.(string); !ok {
				return &ConstraintError{
					Table:      t.Name,
					Column:     col.Name,
					Value:      val,
					Constraint: "type_mismatch",
					Reason:     fmt.Sprintf("expected string, got %T", val),
					RowIndex:   rowIndex,
				}
			}
		case ColumnTypeInt:
			// Normalize to int64 for consistency
			switch v := val.(type) {
			case int:
				row[col.Name] = int64(v) // normalize to int64
			case int64:
				// already int64, perfect
			case float64:
				// JSON numbers come as float64
				if v == float64(int64(v)) {
					row[col.Name] = int64(v)
				} else {
					return &ConstraintError{
						Table:      t.Name,
						Column:     col.Name,
						Value:      val,
						Constraint: "type_mismatch",
						Reason:     fmt.Sprintf("expected integer, got float with decimal: %v", v),
						RowIndex:   rowIndex,
					}
				}
			default:
				return &ConstraintError{
					Table:      t.Name,
					Column:     col.Name,
					Value:      val,
					Constraint: "type_mismatch",
					Reason:     fmt.Sprintf("expected integer, got %T", val),
					RowIndex:   rowIndex,
				}
			}
		case ColumnTypeFloat:
			if _, ok := val.(float64); !ok {
				return typeMismatchError(t.Name, col.Name, val, "Float (number)", rowIndex)
			}

		case ColumnTypeEmail:
			str, ok := val.(string)
			if !ok {
				return typeMismatchError(t.Name, col.Name, val, "string", rowIndex)
			}
			// Validate email format
			if !emailRegex.MatchString(str) {
				return &ConstraintError{
					Table:      t.Name,
					Column:     col.Name,
					Value:      val,
					Constraint: "invalid_email",
					Reason:     "invalid email format",
					RowIndex:   rowIndex,
				}
			}

		case ColumnTypeBool:
			if _, ok := val.(bool); !ok {
				return typeMismatchError(t.Name, col.Name, val, "boolean", rowIndex)
			}

		case ColumnTypeDate, ColumnTypeTime:
			switch v := val.(type) {
			case string:
				_, err := time.Parse("2006-01-02", v)
				if err != nil && col.Type == ColumnTypeTime {
					_, err = time.Parse(time.RFC3339, v)
				}
				if err != nil {
					return typeMismatchError(t.Name, col.Name, val, "date/time string", rowIndex)
				}
			default:
				return typeMismatchError(t.Name, col.Name, val, "date/time string or time.Time", rowIndex)
			}

		default:
			return fmt.Errorf("unknown column type %q", col.Type)
		}
	}

	return nil
}
func typeMismatchError(table, col string, val interface{}, expected string, rowIndex int) *ConstraintError {
	return &ConstraintError{
		Table:      table,
		Column:     col,
		Value:      val,
		Constraint: "type_mismatch",
		Reason:     fmt.Sprintf("expected %s, got %T", expected, val),
		RowIndex:   rowIndex,
	}
}
