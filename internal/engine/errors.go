package engine

import "fmt"

type ConstraintError struct {
	Table  string
	Column string
	Value  interface{}
}

func (e *ConstraintError) Error() string {
	return fmt.Sprintf(
		"constraint violation on %s.%s (value=%v)",
		e.Table, e.Column, e.Value,
	)
}
