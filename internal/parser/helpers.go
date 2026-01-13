package parser

import (
	"github.com/leengari/mini-rdbms/internal/parser/lexer"
)

// isTypedLiteralKeyword checks if a token type is a typed literal keyword (DATE, TIME, EMAIL)
func isTypedLiteralKeyword(t lexer.TokenType) bool {
	return t == lexer.DATE || t == lexer.TIME || t == lexer.EMAIL
}

// isIdentifierOrKeyword checks if a token can be used as an identifier
// Includes IDENTIFIER and typed literal keywords that can be column names
func isIdentifierOrKeyword(t lexer.TokenType) bool {
	return t == lexer.IDENTIFIER || isTypedLiteralKeyword(t)
}

// isNumericType checks if a literal kind represents a numeric type
func isNumericType(kind string) bool {
	return kind == "INT" || kind == "FLOAT"
}

// isComparisonOperator checks if a token type is a comparison operator
func isComparisonOperator(t lexer.TokenType) bool {
	return t == lexer.EQUALS ||
		t == lexer.LESS_THAN ||
		t == lexer.GREATER_THAN ||
		t == lexer.LESS_EQUAL ||
		t == lexer.GREATER_EQUAL ||
		t == lexer.NOT_EQUAL
}

// isLogicalOperator checks if a token type is a logical operator (AND, OR)
func isLogicalOperator(t lexer.TokenType) bool {
	return t == lexer.AND || t == lexer.OR
}
