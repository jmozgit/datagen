package inspector

import "errors"

var (
	ErrEntityNotFound = errors.New("not found")
	ErrEmptySchema    = errors.New("empty working schema")
)
