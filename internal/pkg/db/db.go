package db

import (
	"context"
)

type Row interface {
	Scan(dest ...any) error
}

type Rows interface {
	Close()
	Err() error
	Next() bool
	Scan(dest ...any) error
}

type Connect interface {
	Close(ctx context.Context) error
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	Execute(ctx context.Context, sql string, args ...any) error
}
