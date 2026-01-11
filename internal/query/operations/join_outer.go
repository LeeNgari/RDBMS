package operations

import (
	"log/slog"

	"github.com/leengari/mini-rdbms/internal/domain/data"
	"github.com/leengari/mini-rdbms/internal/domain/schema"
)

// LeftJoin performs a LEFT OUTER JOIN between two tables
// Returns all rows from the left table, with matching rows from the right table
// If no match is found, right table columns are set to NULL
func LeftJoin(
	leftTable *schema.Table,
	rightTable *schema.Table,
	leftColumn string,
	rightColumn string,
	pred JoinPredicateFunc,
) ([]data.JoinedRow, error) {
	// Validate join condition
	if err := validateJoinCondition(leftTable, rightTable, leftColumn, rightColumn); err != nil {
		return nil, err
	}

	// Acquire read locks on both tables
	leftTable.RLock()
	defer leftTable.RUnlock()
	rightTable.RLock()
	defer rightTable.RUnlock()

	slog.Debug("Starting LEFT JOIN",
		slog.String("left_table", leftTable.Name),
		slog.String("right_table", rightTable.Name),
		slog.String("left_column", leftColumn),
		slog.String("right_column", rightColumn),
	)

	// Build hash index on right table
	hashIndex, reusedIndex := buildJoinIndex(rightTable, rightColumn)
	if reusedIndex {
		slog.Debug("Reusing existing index", slog.String("column", rightColumn))
	}

	results := make([]data.JoinedRow, 0)
	matchedLeftRows := make(map[int]bool) // Track which left rows found matches

	// Phase 1: INNER JOIN (matching rows)
	for leftPos, leftRow := range leftTable.Rows {
		leftValue, exists := leftRow[leftColumn]
		if !exists {
			continue // Skip rows with NULL join column
		}

		rightPositions, found := hashIndex[leftValue]
		if found {
			matchedLeftRows[leftPos] = true
			for _, rightPos := range rightPositions {
				rightRow := rightTable.Rows[rightPos]
				joined := combineRows(leftRow, rightRow, leftTable.Name, rightTable.Name)

				if pred == nil || pred(joined) {
					results = append(results, joined)
				}
			}
		}
	}

	// Phase 2: Add unmatched left rows with NULL right columns
	for leftPos, leftRow := range leftTable.Rows {
		if !matchedLeftRows[leftPos] {
			joined := combineRowsWithNull(leftRow, nil, leftTable, rightTable)

			if pred == nil || pred(joined) {
				results = append(results, joined)
			}
		}
	}

	slog.Info("LEFT JOIN completed",
		slog.String("left_table", leftTable.Name),
		slog.String("right_table", rightTable.Name),
		slog.Int("result_rows", len(results)),
		slog.Int("unmatched_left_rows", len(leftTable.Rows)-len(matchedLeftRows)),
	)

	return results, nil
}

// RightJoin performs a RIGHT OUTER JOIN between two tables
// Returns all rows from the right table, with matching rows from the left table
// If no match is found, left table columns are set to NULL
func RightJoin(
	leftTable *schema.Table,
	rightTable *schema.Table,
	leftColumn string,
	rightColumn string,
	pred JoinPredicateFunc,
) ([]data.JoinedRow, error) {
	// Validate join condition
	if err := validateJoinCondition(leftTable, rightTable, leftColumn, rightColumn); err != nil {
		return nil, err
	}

	// Acquire read locks on both tables
	leftTable.RLock()
	defer leftTable.RUnlock()
	rightTable.RLock()
	defer rightTable.RUnlock()

	slog.Debug("Starting RIGHT JOIN",
		slog.String("left_table", leftTable.Name),
		slog.String("right_table", rightTable.Name),
		slog.String("left_column", leftColumn),
		slog.String("right_column", rightColumn),
	)

	// Build hash index on right table
	hashIndex, reusedIndex := buildJoinIndex(rightTable, rightColumn)
	if reusedIndex {
		slog.Debug("Reusing existing index", slog.String("column", rightColumn))
	}

	results := make([]data.JoinedRow, 0)
	matchedRightRows := make(map[int]bool) // Track which right rows found matches

	// Phase 1: INNER JOIN (matching rows)
	for _, leftRow := range leftTable.Rows {
		leftValue, exists := leftRow[leftColumn]
		if !exists {
			continue // Skip rows with NULL join column
		}

		rightPositions, found := hashIndex[leftValue]
		if found {
			for _, rightPos := range rightPositions {
				matchedRightRows[rightPos] = true
				rightRow := rightTable.Rows[rightPos]
				joined := combineRows(leftRow, rightRow, leftTable.Name, rightTable.Name)

				if pred == nil || pred(joined) {
					results = append(results, joined)
				}
			}
		}
	}

	// Phase 2: Add unmatched right rows with NULL left columns
	for rightPos, rightRow := range rightTable.Rows {
		if !matchedRightRows[rightPos] {
			joined := combineRowsWithNull(nil, rightRow, leftTable, rightTable)

			if pred == nil || pred(joined) {
				results = append(results, joined)
			}
		}
	}

	slog.Info("RIGHT JOIN completed",
		slog.String("left_table", leftTable.Name),
		slog.String("right_table", rightTable.Name),
		slog.Int("result_rows", len(results)),
		slog.Int("unmatched_right_rows", len(rightTable.Rows)-len(matchedRightRows)),
	)

	return results, nil
}

// FullJoin performs a FULL OUTER JOIN between two tables
// Returns all rows from both tables
// If no match is found, the unmatched table's columns are set to NULL
func FullJoin(
	leftTable *schema.Table,
	rightTable *schema.Table,
	leftColumn string,
	rightColumn string,
	pred JoinPredicateFunc,
) ([]data.JoinedRow, error) {
	// Validate join condition
	if err := validateJoinCondition(leftTable, rightTable, leftColumn, rightColumn); err != nil {
		return nil, err
	}

	// Acquire read locks on both tables
	leftTable.RLock()
	defer leftTable.RUnlock()
	rightTable.RLock()
	defer rightTable.RUnlock()

	slog.Debug("Starting FULL OUTER JOIN",
		slog.String("left_table", leftTable.Name),
		slog.String("right_table", rightTable.Name),
		slog.String("left_column", leftColumn),
		slog.String("right_column", rightColumn),
	)

	// Build hash index on right table
	hashIndex, reusedIndex := buildJoinIndex(rightTable, rightColumn)
	if reusedIndex {
		slog.Debug("Reusing existing index", slog.String("column", rightColumn))
	}

	results := make([]data.JoinedRow, 0)
	matchedLeftRows := make(map[int]bool)
	matchedRightRows := make(map[int]bool)

	// Phase 1: INNER JOIN (matching rows)
	for leftPos, leftRow := range leftTable.Rows {
		leftValue, exists := leftRow[leftColumn]
		if !exists {
			continue // Skip rows with NULL join column
		}

		rightPositions, found := hashIndex[leftValue]
		if found {
			matchedLeftRows[leftPos] = true
			for _, rightPos := range rightPositions {
				matchedRightRows[rightPos] = true
				rightRow := rightTable.Rows[rightPos]
				joined := combineRows(leftRow, rightRow, leftTable.Name, rightTable.Name)

				if pred == nil || pred(joined) {
					results = append(results, joined)
				}
			}
		}
	}

	// Phase 2: Add unmatched left rows with NULL right columns
	for leftPos, leftRow := range leftTable.Rows {
		if !matchedLeftRows[leftPos] {
			joined := combineRowsWithNull(leftRow, nil, leftTable, rightTable)

			if pred == nil || pred(joined) {
				results = append(results, joined)
			}
		}
	}

	// Phase 3: Add unmatched right rows with NULL left columns
	for rightPos, rightRow := range rightTable.Rows {
		if !matchedRightRows[rightPos] {
			joined := combineRowsWithNull(nil, rightRow, leftTable, rightTable)

			if pred == nil || pred(joined) {
				results = append(results, joined)
			}
		}
	}

	slog.Info("FULL OUTER JOIN completed",
		slog.String("left_table", leftTable.Name),
		slog.String("right_table", rightTable.Name),
		slog.Int("result_rows", len(results)),
		slog.Int("unmatched_left_rows", len(leftTable.Rows)-len(matchedLeftRows)),
		slog.Int("unmatched_right_rows", len(rightTable.Rows)-len(matchedRightRows)),
	)

	return results, nil
}
