package parser

import (
	"github.com/leengari/mini-rdbms/internal/validation"
)

// validateDate wraps validation.ValidateDate for backward compatibility
func validateDate(value string) error {
	return validation.ValidateDate(value)
}

// validateTime wraps validation.ValidateTime for backward compatibility
func validateTime(value string) error {
	return validation.ValidateTime(value)
}

// validateEmail wraps validation.ValidateEmail for backward compatibility
func validateEmail(value string) error {
	return validation.ValidateEmail(value)
}
