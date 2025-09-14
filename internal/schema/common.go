package schema

import "errors"

var (
	ErrEntityNotFound  = errors.New("not found")
	ErrEmptySchema     = errors.New("empty working schema")
	ErrUnsupportedType = errors.New("unsupported type")
)
