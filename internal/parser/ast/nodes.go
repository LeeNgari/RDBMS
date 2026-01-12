package ast

import (
	"bytes"
	"fmt"
)

// Node is the base interface for all AST nodes
type Node interface {
	TokenLiteral() string
	String() string
}

// Statement represents a standalone SQL statement (SELECT, INSERT, etc.)
type Statement interface {
	Node
	statementNode()
}

// Expression represents a value or operation
type Expression interface {
	Node
	expressionNode()
}

// Identifier represents a column or table name
type Identifier struct {
	TokenLiteralValue string // The token literal (e.g. "users")
	Value             string // The value (e.g. "users")
}

func (i *Identifier) expressionNode()      {}
func (i *Identifier) TokenLiteral() string { return i.TokenLiteralValue }
func (i *Identifier) String() string       { return i.Value }

// Literal represents a fixed value (string, number)
type Literal struct {
	TokenLiteralValue string
	Value             interface{} // string, int, float64
	Kind              int         // 0=String, 1=Int, 2=Float
}

func (l *Literal) expressionNode()      {}
func (l *Literal) TokenLiteral() string { return l.TokenLiteralValue }
func (l *Literal) String() string       { return l.TokenLiteralValue }

// SelectStatement: SELECT col1, col2 FROM table WHERE ...
type SelectStatement struct {
	Fields    []*Identifier
	TableName *Identifier
	Where     Expression // For now, simple binary expression or nil
}

func (s *SelectStatement) statementNode()       {}
func (s *SelectStatement) TokenLiteral() string { return "SELECT" }
func (s *SelectStatement) String() string {
	var out bytes.Buffer
	out.WriteString("SELECT ")
	for i, f := range s.Fields {
		out.WriteString(f.String())
		if i < len(s.Fields)-1 {
			out.WriteString(", ")
		}
	}
	out.WriteString(" FROM ")
	out.WriteString(s.TableName.String())
	if s.Where != nil {
		out.WriteString(" WHERE ")
		out.WriteString(s.Where.String())
	}
	return out.String()
}

// InsertStatement: INSERT INTO table (col1, col2) VALUES (val1, val2)
type InsertStatement struct {
	TableName *Identifier
	Columns   []*Identifier
	Values    []Expression
}

func (s *InsertStatement) statementNode()       {}
func (s *InsertStatement) TokenLiteral() string { return "INSERT" }
func (s *InsertStatement) String() string {
	var out bytes.Buffer
	out.WriteString("INSERT INTO ")
	out.WriteString(s.TableName.String())
	out.WriteString(" (")
	for i, c := range s.Columns {
		out.WriteString(c.String())
		if i < len(s.Columns)-1 {
			out.WriteString(", ")
		}
	}
	out.WriteString(") VALUES (")
	for i, v := range s.Values {
		out.WriteString(v.String())
		if i < len(s.Values)-1 {
			out.WriteString(", ")
		}
	}
	out.WriteString(")")
	return out.String()
}

// BinaryExpression: Left Operator Right (e.g. id = 1)
type BinaryExpression struct {
	Left     Expression
	Operator string
	Right    Expression
}

func (e *BinaryExpression) expressionNode()      {}
func (e *BinaryExpression) TokenLiteral() string { return e.Operator }
func (e *BinaryExpression) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left.String(), e.Operator, e.Right.String())
}
