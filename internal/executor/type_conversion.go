package executor

import (
	"fmt"

	"github.com/leengari/mini-rdbms/internal/domain/schema"
	"github.com/leengari/mini-rdbms/internal/parser/ast"
	"github.com/leengari/mini-rdbms/internal/validation"
)

// convertLiteralToSchemaType attempts to convert a literal to match the schema type.
// If literal is already correct type, returns as-is.
// If literal is STRING and schema expects DATE/TIME/EMAIL, validates and converts.
// This enables implicit type detection based on schema.
func convertLiteralToSchemaType(lit *ast.Literal, schemaType schema.ColumnType) (*ast.Literal, error) {
	// If types already match, no conversion needed
	if typesMatch(lit.Kind, schemaType) {
		return lit, nil
	}

	// Only convert STRING literals to typed literals
	if lit.Kind != ast.LiteralString {
		return nil, fmt.Errorf("expected %s, got %s", schemaType, lit.Kind)
	}

	// Get string value
	strValue, ok := lit.Value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string value for conversion")
	}

	// Attempt conversion based on schema type
	switch schemaType {
	case schema.ColumnTypeDate:
		if err := validation.ValidateDate(strValue); err != nil {
			return nil, fmt.Errorf("invalid date: %w", err)
		}
		return &ast.Literal{
			TokenLiteralValue: lit.TokenLiteralValue,
			Value:             strValue,
			Kind:              ast.LiteralDate,
		}, nil

	case schema.ColumnTypeTime:
		if err := validation.ValidateTime(strValue); err != nil {
			return nil, fmt.Errorf("invalid time: %w", err)
		}
		return &ast.Literal{
			TokenLiteralValue: lit.TokenLiteralValue,
			Value:             strValue,
			Kind:              ast.LiteralTime,
		}, nil

	case schema.ColumnTypeEmail:
		if err := validation.ValidateEmail(strValue); err != nil {
			return nil, fmt.Errorf("invalid email: %w", err)
		}
		return &ast.Literal{
			TokenLiteralValue: lit.TokenLiteralValue,
			Value:             strValue,
			Kind:              ast.LiteralEmail,
		}, nil

	case schema.ColumnTypeText:
		// TEXT accepts any string
		return lit, nil

	case schema.ColumnTypeInt:
		return nil, fmt.Errorf("cannot convert string to INT (got '%s')", strValue)

	case schema.ColumnTypeFloat:
		return nil, fmt.Errorf("cannot convert string to FLOAT (got '%s')", strValue)

	case schema.ColumnTypeBool:
		return nil, fmt.Errorf("cannot convert string to BOOL (got '%s')", strValue)

	default:
		return nil, fmt.Errorf("cannot convert STRING to %s", schemaType)
	}
}

// typesMatch checks if a literal kind matches a schema column type
func typesMatch(kind ast.LiteralKind, schemaType schema.ColumnType) bool {
	switch schemaType {
	case schema.ColumnTypeInt:
		return kind == ast.LiteralInt
	case schema.ColumnTypeFloat:
		// Float columns accept both INT and FLOAT literals
		return kind == ast.LiteralInt || kind == ast.LiteralFloat
	case schema.ColumnTypeText:
		return kind == ast.LiteralString
	case schema.ColumnTypeBool:
		return kind == ast.LiteralBool
	case schema.ColumnTypeDate:
		return kind == ast.LiteralDate
	case schema.ColumnTypeTime:
		return kind == ast.LiteralTime
	case schema.ColumnTypeEmail:
		return kind == ast.LiteralEmail
	default:
		return false
	}
}
