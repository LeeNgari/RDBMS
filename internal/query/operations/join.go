package operations

import (
	"fmt"
	"log/slog"

	"github.com/leengari/mini-rdbms/internal/domain/data"
	"github.com/leengari/mini-rdbms/internal/domain/schema"
)

// Tests whether a joined row matches certain criteria
type JoinPredicateFunc func(data.JoinedRow) bool

// Join performs an INNER JOIN between two tables using hash join algorithm
// Returns rows where leftTable[leftColumn] = rightTable[rightColumn]
// Applies optional predicate filter during probe phase for efficiency
func Join(
	leftTable *schema.Table,
	rightTable *schema.Table,
	leftColumn string,
	rightColumn string,
	pred JoinPredicateFunc,
) ([]data.JoinedRow, error) {
	
	if err := validateJoinCondition(leftTable, rightTable, leftColumn, rightColumn); err != nil {
		return nil, err
	}

	// Acquire read locks on both tables
	leftTable.RLock()
	defer leftTable.RUnlock()
	rightTable.RLock()
	defer rightTable.RUnlock()

	slog.Debug("Starting INNER JOIN",
		slog.String("left_table", leftTable.Name),
		slog.String("right_table", rightTable.Name),
		slog.String("left_column", leftColumn),
		slog.String("right_column", rightColumn),
		slog.Int("left_rows", len(leftTable.Rows)),
		slog.Int("right_rows", len(rightTable.Rows)),
	)

	// Build hash index on right table
	hashIndex, reusedIndex := buildJoinIndex(rightTable, rightColumn)
	if reusedIndex {
		slog.Debug("Reusing existing index", slog.String("column", rightColumn))
	} else {
		slog.Debug("Built temporary index", slog.String("column", rightColumn))
	}

	//Probe left table and combine matches
	results := make([]data.JoinedRow, 0)
	skippedByPredicate := 0

	for _, leftRow := range leftTable.Rows {
		// Get join value from left row
		leftValue, exists := leftRow[leftColumn]
		if !exists {
			continue // Skip rows with NULL join column
		}

		// Look up matching rows in hash index
		rightPositions, found := hashIndex[leftValue]
		if !found {
			continue // No matches (INNER JOIN excludes)
		}

		// Combine with each matching right row
		for _, rightPos := range rightPositions {
			rightRow := rightTable.Rows[rightPos]

			// Combine rows
			joinedRow := combineRows(leftRow, rightRow, leftTable.Name, rightTable.Name)

			// Apply predicate during probe phase (optimization!)
			if pred != nil && !pred(joinedRow) {
				skippedByPredicate++
				continue
			}

			results = append(results, joinedRow)
		}
	}

	slog.Info("INNER JOIN completed",
		slog.String("left_table", leftTable.Name),
		slog.String("right_table", rightTable.Name),
		slog.Int("result_rows", len(results)),
		slog.Int("filtered_by_predicate", skippedByPredicate),
	)

	return results, nil
}

// JoinWhere is a convenience wrapper that performs JOIN with a predicate
func JoinWhere(
	leftTable *schema.Table,
	rightTable *schema.Table,
	leftColumn string,
	rightColumn string,
	pred JoinPredicateFunc,
) ([]data.JoinedRow, error) {
	return Join(leftTable, rightTable, leftColumn, rightColumn, pred)
}

// validateJoinCondition checks if the join is valid
func validateJoinCondition(
	leftTable *schema.Table,
	rightTable *schema.Table,
	leftColumn string,
	rightColumn string,
) error {
	if leftTable == nil {
		return fmt.Errorf("left table is nil")
	}
	if rightTable == nil {
		return fmt.Errorf("right table is nil")
	}

	// Find columns in schemas
	var leftCol, rightCol *schema.Column
	for i := range leftTable.Schema.Columns {
		if leftTable.Schema.Columns[i].Name == leftColumn {
			leftCol = &leftTable.Schema.Columns[i]
			break
		}
	}
	for i := range rightTable.Schema.Columns {
		if rightTable.Schema.Columns[i].Name == rightColumn {
			rightCol = &rightTable.Schema.Columns[i]
			break
		}
	}

	if leftCol == nil {
		return fmt.Errorf("column '%s' not found in table '%s'", leftColumn, leftTable.Name)
	}
	if rightCol == nil {
		return fmt.Errorf("column '%s' not found in table '%s'", rightColumn, rightTable.Name)
	}

	// Validate type compatibility
	if leftCol.Type != rightCol.Type {
		return fmt.Errorf("cannot join incompatible types: %s.%s (%s) with %s.%s (%s)",
			leftTable.Name, leftColumn, leftCol.Type,
			rightTable.Name, rightColumn, rightCol.Type,
		)
	}

	// Warn if joining on non-indexed columns
	if _, leftIndexed := leftTable.Indexes[leftColumn]; !leftIndexed {
		slog.Warn("Joining on non-indexed column (consider adding index)",
			slog.String("table", leftTable.Name),
			slog.String("column", leftColumn),
		)
	}
	if _, rightIndexed := rightTable.Indexes[rightColumn]; !rightIndexed {
		slog.Warn("Joining on non-indexed column (consider adding index)",
			slog.String("table", rightTable.Name),
			slog.String("column", rightColumn),
		)
	}

	return nil
}

// buildJoinIndex creates a hash index for the join column
// Returns the index and a boolean indicating if an existing index was reused
func buildJoinIndex(table *schema.Table, columnName string) (map[interface{}][]int, bool) {
	// Try to reuse existing index
	if idx, exists := table.Indexes[columnName]; exists {
		return idx.Data, true
	}

	// Build temporary index
	hashIndex := make(map[interface{}][]int)
	for i, row := range table.Rows {
		value, exists := row[columnName]
		if !exists {
			continue // Skip NULL values
		}
		hashIndex[value] = append(hashIndex[value], i)
	}

	return hashIndex, false
}

// combineRows merges two rows with table-qualified column names
func combineRows(
	leftRow data.Row,
	rightRow data.Row,
	leftTableName string,
	rightTableName string,
) data.JoinedRow {
	joined := data.NewJoinedRow()

	// Add left table columns with prefix
	for colName, value := range leftRow {
		qualifiedName := fmt.Sprintf("%s.%s", leftTableName, colName)
		joined.Set(qualifiedName, value)
	}

	// Add right table columns with prefix
	for colName, value := range rightRow {
		qualifiedName := fmt.Sprintf("%s.%s", rightTableName, colName)
		joined.Set(qualifiedName, value)
	}

	return joined
}
