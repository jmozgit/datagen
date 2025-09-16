package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/xrand"

	"github.com/jackc/pgx/v5"
	"github.com/samber/lo"
)

type Conn struct {
	conn *pgx.Conn
}

func New(t *testing.T, connStr string) (*Conn, error) {
	t.Helper()

	ctx := t.Context()

	cfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("%w parse config", err)
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: postgres new", err)
	}

	dbname, err := createTempDB(ctx, conn)
	if err != nil {
		conn.Close(ctx)

		return nil, fmt.Errorf("%w: postgres new", err)
	}

	cfg.Database = dbname
	tempConn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		conn.Close(ctx)

		return nil, fmt.Errorf("%w: connect config", err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		if err = tempConn.Close(ctx); err != nil {
			t.Errorf("failed to close temp conn: %v", err)
		}

		_, err := conn.Exec(ctx, fmt.Sprintf("DROP DATABASE %s WITH (force)", dbname))
		if err != nil {
			t.Errorf("failed to drop database %s: %v", dbname, err)
		}

		if err = conn.Close(ctx); err != nil {
			t.Errorf("failed to close conn: %v", err)
		}
	})

	return &Conn{conn: tempConn}, nil
}

func createTempDB(ctx context.Context, conn *pgx.Conn) (string, error) {
	dbname := genDBName()
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbname))
	if err != nil {
		return "", fmt.Errorf("%w: create temp db", err)
	}

	return dbname, nil
}

func (c *Conn) Raw() *pgx.Conn {
	return c.conn
}

type partPolicy struct {
	Method string
	Cnt    int
	Field  string
}

type CreateTableOptions struct {
	pks        []string
	partPolicy partPolicy
}

type CreateTableOption func(c *CreateTableOptions)

func WithPKs(pks []string) CreateTableOption {
	return func(c *CreateTableOptions) {
		c.pks = pks
	}
}

func WithHashPartitions(parts int, field string) CreateTableOption {
	return func(c *CreateTableOptions) {
		c.partPolicy = partPolicy{
			Method: "hash",
			Cnt:    parts,
			Field:  field,
		}
	}
}

func (c *Conn) CreateTable(ctx context.Context, table model.Table, opts ...CreateTableOption) error {
	if err := c.ensureUnexistence(ctx, table.Name); err != nil {
		return fmt.Errorf("%w: ensure unexistence %s", err, table.Name)
	}

	params := CreateTableOptions{
		pks: make([]string, 0),
		partPolicy: partPolicy{
			Method: "",
			Cnt:    0,
			Field:  "",
		},
	}
	for _, opt := range opts {
		opt(&params)
	}

	query := fmt.Sprintf("create table %s (", table.Name.String())
	query += strings.Join(lo.Map(table.Columns, func(c model.Column, _ int) string {
		return fmt.Sprintf("%s %s", c.Name, c.Type)
	}), ",")

	if len(params.pks) != 0 {
		query += fmt.Sprintf(",primary key (%s)", strings.Join(params.pks, ","))
	}

	query += ")"

	if params.partPolicy.Method != "" {
		query += fmt.Sprintf("partition by hash(%s)", params.partPolicy.Field)
		if _, err := c.conn.Exec(ctx, query); err != nil {
			return fmt.Errorf("%w: create table", err)
		}

		for i := range params.partPolicy.Cnt {
			part := fmt.Sprintf(
				"create table %s_part_%d partition of %s for values with (modulus %d, remainder %d)",
				table.Name.String(), i, table.Name.String(), params.partPolicy.Cnt, i,
			)

			if _, err := c.conn.Exec(ctx, part); err != nil {
				return fmt.Errorf("%w: create table", err)
			}
		}

		return nil
	}

	if _, err := c.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("%w: create table", err)
	}

	return nil
}

func (c *Conn) ensureUnexistence(ctx context.Context, name model.TableName) error {
	_, err := c.conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	if err != nil {
		return fmt.Errorf("%w: ensure unexistence", err)
	}

	return nil
}

func genDBName() string {
	const dbNameLen = 10

	return xrand.LowerCaseString(dbNameLen)
}
