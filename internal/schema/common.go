package schema

import "errors"

var (
	ErrEntityNotFound        = errors.New("not found")
	ErrEmptySchema           = errors.New("empty working schema")
	ErrUnsupportedType       = errors.New("unsupported type")
	ErrTooManyTablesMatched  = errors.New("too many tables matched")
	ErrTooManyColumnsMatched = errors.New("too many tables matched")
)
