package parser

import (
	"fmt"
	"strconv"

	"github.com/leengari/mini-rdbms/internal/parser/ast"
	"github.com/leengari/mini-rdbms/internal/parser/lexer"
)

type Parser struct {
	tokens  []lexer.Token
	curPos  int
	curTok  lexer.Token
	peekTok lexer.Token
}

func New(tokens []lexer.Token) *Parser {
	p := &Parser{tokens: tokens, curPos: 0}
	// Read two tokens to set curTok and peekTok
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.curTok = p.peekTok
	if p.curPos < len(p.tokens) {
		p.peekTok = p.tokens[p.curPos]
		p.curPos++
	} else {
		p.peekTok = lexer.Token{Type: lexer.EOF}
	}
}

func (p *Parser) Parse() (ast.Statement, error) {
	switch p.curTok.Type {
	case lexer.SELECT:
		return p.parseSelect()
	case lexer.INSERT:
		return p.parseInsert()
	default:
		return nil, fmt.Errorf("unexpected token %v, expected SELECT or INSERT", p.curTok.Type)
	}
}

func (p *Parser) parseSelect() (*ast.SelectStatement, error) {
	stmt := &ast.SelectStatement{}

	// SELECT
	p.nextToken()

	// Fields
	fields, err := p.parseIdentifierList()
	if err != nil {
		return nil, err
	}
	stmt.Fields = fields

	// FROM
	if p.curTok.Type != lexer.FROM {
		return nil, fmt.Errorf("expected FROM, got %s", p.curTok.Literal)
	}
	p.nextToken()

	// Table Name
	if p.curTok.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", p.curTok.Literal)
	}
	stmt.TableName = &ast.Identifier{TokenLiteralValue: p.curTok.Literal, Value: p.curTok.Literal}
	p.nextToken()

	// WHERE (Optional)
	if p.curTok.Type == lexer.WHERE {
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// Semicolon (Optional)
	if p.curTok.Type == lexer.SEMICOLON {
		p.nextToken()
	}

	return stmt, nil
}

func (p *Parser) parseInsert() (*ast.InsertStatement, error) {
	stmt := &ast.InsertStatement{}

	// INSERT
	p.nextToken()

	// INTO
	if p.curTok.Type != lexer.INTO {
		return nil, fmt.Errorf("expected INTO, got %s", p.curTok.Literal)
	}
	p.nextToken()

	// Table Name
	if p.curTok.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", p.curTok.Literal)
	}
	stmt.TableName = &ast.Identifier{TokenLiteralValue: p.curTok.Literal, Value: p.curTok.Literal}
	p.nextToken()

	// Columns (Optional but we'll require them for now or handle parens)
	if p.curTok.Type == lexer.PAREN_OPEN {
		// Parse columns
		cols, err := p.parseIdentifierList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = cols
	}

	// VALUES
	if p.curTok.Type != lexer.VALUES {
		return nil, fmt.Errorf("expected VALUES, got %s", p.curTok.Literal)
	}
	p.nextToken()

	// (
	if p.curTok.Type != lexer.PAREN_OPEN {
		return nil, fmt.Errorf("expected (, got %s", p.curTok.Literal)
	}
	
	// Parse Values List
	values, err := p.parseExpressionList()
	if err != nil {
		return nil, err
	}
	stmt.Values = values

	// Semicolon (Optional)
	if p.curTok.Type == lexer.SEMICOLON {
		p.nextToken()
	}

	return stmt, nil
}

func (p *Parser) parseIdentifierList() ([]*ast.Identifier, error) {
	var identifiers []*ast.Identifier

	// Handle first identifier or *
	if p.curTok.Type == lexer.ASTERISK {
		identifiers = append(identifiers, &ast.Identifier{TokenLiteralValue: "*", Value: "*"})
		p.nextToken()
		return identifiers, nil
	}

	// Handle ( for column list in INSERT
	if p.curTok.Type == lexer.PAREN_OPEN {
		p.nextToken()
	}

	if p.curTok.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected identifier, got %s", p.curTok.Literal)
	}

	identifiers = append(identifiers, &ast.Identifier{TokenLiteralValue: p.curTok.Literal, Value: p.curTok.Literal})
	p.nextToken()

	for p.curTok.Type == lexer.COMMA {
		p.nextToken()
		if p.curTok.Type != lexer.IDENTIFIER {
			return nil, fmt.Errorf("expected identifier after comma, got %s", p.curTok.Literal)
		}
		identifiers = append(identifiers, &ast.Identifier{TokenLiteralValue: p.curTok.Literal, Value: p.curTok.Literal})
		p.nextToken()
	}

	// Handle ) for column list in INSERT
	if p.curTok.Type == lexer.PAREN_CLOSE {
		p.nextToken()
	}

	return identifiers, nil
}

func (p *Parser) parseExpressionList() ([]ast.Expression, error) {
	var list []ast.Expression

	if p.curTok.Type == lexer.PAREN_OPEN {
		p.nextToken()
	}

	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	list = append(list, expr)

	for p.curTok.Type == lexer.COMMA {
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		list = append(list, expr)
	}

	if p.curTok.Type == lexer.PAREN_CLOSE {
		p.nextToken()
	}

	return list, nil
}

// Minimal expression parser (only supports: ident = val, or just val)
func (p *Parser) parseExpression() (ast.Expression, error) {
	// Left side
	left, err := p.parseAtom()
	if err != nil {
		return nil, err
	}

	// Check for operator
	if p.curTok.Type == lexer.EQUALS {
		op := p.curTok.Literal
		p.nextToken()
		right, err := p.parseAtom()
		if err != nil {
			return nil, err
		}
		return &ast.BinaryExpression{Left: left, Operator: op, Right: right}, nil
	}

	return left, nil
}

func (p *Parser) parseAtom() (ast.Expression, error) {
	switch p.curTok.Type {
	case lexer.IDENTIFIER:
		val := p.curTok.Literal
		p.nextToken()
		return &ast.Identifier{TokenLiteralValue: val, Value: val}, nil
	case lexer.STRING:
		val := p.curTok.Literal
		p.nextToken()
		return &ast.Literal{TokenLiteralValue: val, Value: val, Kind: 0}, nil
	case lexer.NUMBER:
		valStr := p.curTok.Literal
		p.nextToken()
		// Try int
		if i, err := strconv.Atoi(valStr); err == nil {
			return &ast.Literal{TokenLiteralValue: valStr, Value: i, Kind: 1}, nil
		}
		// Try float
		if f, err := strconv.ParseFloat(valStr, 64); err == nil {
			return &ast.Literal{TokenLiteralValue: valStr, Value: f, Kind: 2}, nil
		}
		return nil, fmt.Errorf("invalid number: %s", valStr)
	case lexer.TRUE:
		p.nextToken()
		return &ast.Literal{TokenLiteralValue: "TRUE", Value: true, Kind: 3}, nil
	case lexer.FALSE:
		p.nextToken()
		return &ast.Literal{TokenLiteralValue: "FALSE", Value: false, Kind: 3}, nil
	default:
		return nil, fmt.Errorf("unexpected token in expression: %s", p.curTok.Literal)
	}
}
