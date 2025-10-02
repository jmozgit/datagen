package db

import (
	"context"
)

type Row interface {
	Scan(dest ...any) error
}

type Connect interface {
	QueryRow(ctx context.Context, sql string, args ...any) Row
}
