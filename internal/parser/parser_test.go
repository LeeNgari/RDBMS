package parser

import (
	"testing"

	"github.com/leengari/mini-rdbms/internal/parser/ast"
	"github.com/leengari/mini-rdbms/internal/parser/lexer"
)

func TestParseSelect(t *testing.T) {
	input := "SELECT id, name FROM users WHERE id = 1;"
	tokens, err := lexer.Tokenize(input)
	if err != nil {
		t.Fatalf("Lexer error: %v", err)
	}

	p := New(tokens)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", stmt)
	}

	if len(sel.Fields) != 2 {
		t.Fatalf("Expected 2 fields, got %d", len(sel.Fields))
	}
	if sel.Fields[0].Value != "id" {
		t.Errorf("Expected field 0 to be id, got %s", sel.Fields[0].Value)
	}
	if sel.Fields[1].Value != "name" {
		t.Errorf("Expected field 1 to be name, got %s", sel.Fields[1].Value)
	}

	if sel.TableName.Value != "users" {
		t.Errorf("Expected table users, got %s", sel.TableName.Value)
	}

	if sel.Where == nil {
		t.Fatal("Expected Where clause, got nil")
	}

	binExpr, ok := sel.Where.(*ast.BinaryExpression)
	if !ok {
		t.Fatalf("Expected BinaryExpression in Where, got %T", sel.Where)
	}

	if binExpr.Left.(*ast.Identifier).Value != "id" {
		t.Errorf("Expected left side id, got %s", binExpr.Left)
	}
	if binExpr.Operator != "=" {
		t.Errorf("Expected operator =, got %s", binExpr.Operator)
	}
	if binExpr.Right.(*ast.Literal).Value.(int) != 1 {
		t.Errorf("Expected right side 1, got %v", binExpr.Right)
	}
}

func TestParseInsert(t *testing.T) {
	input := "INSERT INTO items (name, price) VALUES ('apple', 1.23);"
	tokens, err := lexer.Tokenize(input)
	if err != nil {
		t.Fatalf("Lexer error: %v", err)
	}

	p := New(tokens)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	ins, ok := stmt.(*ast.InsertStatement)
	if !ok {
		t.Fatalf("Expected InsertStatement, got %T", stmt)
	}

	if ins.TableName.Value != "items" {
		t.Errorf("Expected table items, got %s", ins.TableName.Value)
	}

	if len(ins.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(ins.Columns))
	}
	if ins.Columns[0].Value != "name" {
		t.Errorf("Expected col 0 to be name, got %s", ins.Columns[0].Value)
	}

	if len(ins.Values) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(ins.Values))
	}
	
	val1, ok := ins.Values[0].(*ast.Literal)
	if !ok || val1.Value != "apple" {
		t.Errorf("Expected value 0 to be 'apple', got %v", ins.Values[0])
	}

	val2, ok := ins.Values[1].(*ast.Literal)
	if !ok || val2.Value != 1.23 {
		t.Errorf("Expected value 1 to be 1.23, got %v", ins.Values[1])
	}
}
