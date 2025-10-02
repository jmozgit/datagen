package pgx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
)

type adapterConn struct {
	conn *pgx.Conn
}

func (a adapterConn) QueryRow(ctx context.Context, sql string, args ...any) db.Row {
	row := a.conn.QueryRow(ctx, sql, args...)
	return row
}

func (a adapterConn) Query(ctx context.Context, sql string, args ...any) (db.Rows, error) {
	rows, err := a.conn.Query(ctx, sql, args...)

	return rows, err
}

func NewAdapterConn(conn *pgx.Conn) db.Connect {
	return adapterConn{conn: conn}
}

type adapterPool struct {
	pool *pgxpool.Pool
}

func NewAdapterPool(pool *pgxpool.Pool) db.Connect {
	return adapterPool{pool: pool}
}

func (a adapterPool) QueryRow(ctx context.Context, sql string, args ...any) db.Row {
	row := a.pool.QueryRow(ctx, sql, args...)
	return row
}

func (a adapterPool) Query(ctx context.Context, sql string, args ...any) (db.Rows, error) {
	rows, err := a.pool.Query(ctx, sql, args...)

	return rows, err
}
