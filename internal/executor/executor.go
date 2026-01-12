package executor

import (
	"fmt"

	"github.com/leengari/mini-rdbms/internal/domain/data"
	"github.com/leengari/mini-rdbms/internal/domain/schema"
	"github.com/leengari/mini-rdbms/internal/parser/ast"
	"github.com/leengari/mini-rdbms/internal/query/operations/crud"
	"github.com/leengari/mini-rdbms/internal/query/operations/projection"
)

type Result struct {
	Columns []string
	Rows    []data.Row
	Message string
}

func Execute(stmt ast.Statement, db *schema.Database) (*Result, error) {
	switch s := stmt.(type) {
	case *ast.SelectStatement:
		return executeSelect(s, db)
	case *ast.InsertStatement:
		return executeInsert(s, db)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func executeSelect(stmt *ast.SelectStatement, db *schema.Database) (*Result, error) {
	tableName := stmt.TableName.Value
	table, ok := db.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	// Build Projection
	var proj *projection.Projection
	var columns []string

	// Check for SELECT *
	if len(stmt.Fields) == 1 && stmt.Fields[0].Value == "*" {
		proj = projection.NewProjection()
		// Get all columns from schema for result header
		for _, col := range table.Schema.Columns {
			columns = append(columns, col.Name)
		}
	} else {
		proj = &projection.Projection{
			SelectAll: false,
			Columns:   make([]projection.ColumnRef, len(stmt.Fields)),
		}
		for i, f := range stmt.Fields {
			proj.Columns[i] = projection.ColumnRef{Column: f.Value}
			columns = append(columns, f.Value)
		}
	}

	var rows []data.Row

	if stmt.Where == nil {
		rows = crud.SelectAll(table, proj)
	} else {
		pred, err := buildPredicate(stmt.Where)
		if err != nil {
			return nil, err
		}
		rows = crud.SelectWhere(table, pred, proj)
	}

	return &Result{
		Columns: columns,
		Rows:    rows,
		Message: fmt.Sprintf("Returned %d rows", len(rows)),
	}, nil
}

func executeInsert(stmt *ast.InsertStatement, db *schema.Database) (*Result, error) {
	tableName := stmt.TableName.Value
	table, ok := db.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	if len(stmt.Columns) != len(stmt.Values) {
		return nil, fmt.Errorf("column count (%d) does not match value count (%d)", len(stmt.Columns), len(stmt.Values))
	}

	row := make(data.Row)
	for i, col := range stmt.Columns {
		valExpr := stmt.Values[i]
		lit, ok := valExpr.(*ast.Literal)
		if !ok {
			return nil, fmt.Errorf("only literal values supported in INSERT for now")
		}
		row[col.Value] = lit.Value
	}

	if err := crud.Insert(table, row); err != nil {
		return nil, err
	}

	return &Result{
		Message: "INSERT 1",
	}, nil
}

func buildPredicate(expr ast.Expression) (crud.PredicateFunc, error) {
	binExpr, ok := expr.(*ast.BinaryExpression)
	if !ok {
		return nil, fmt.Errorf("only binary expressions supported in WHERE clause")
	}

	if binExpr.Operator != "=" {
		return nil, fmt.Errorf("only '=' operator supported in WHERE clause")
	}

	leftIdent, ok := binExpr.Left.(*ast.Identifier)
	if !ok {
		return nil, fmt.Errorf("left side of expression must be an identifier")
	}

	rightLit, ok := binExpr.Right.(*ast.Literal)
	if !ok {
		return nil, fmt.Errorf("right side of expression must be a literal")
	}

	colName := leftIdent.Value
	targetVal := rightLit.Value

	return func(row data.Row) bool {
		val, ok := row[colName]
		if !ok {
			return false
		}
		
		// Handle numeric comparison specifically if needed
		if n1, ok := normalizeToFloat(val); ok {
			if n2, ok := normalizeToFloat(targetVal); ok {
				return n1 == n2
			}
		}

		return val == targetVal
	}, nil
}

func normalizeToFloat(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case float64:
		return val, true
	}
	return 0, false
}
