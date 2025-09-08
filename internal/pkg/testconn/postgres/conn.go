package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/samber/lo"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/xrand"
)

type Conn struct {
	conn *pgx.Conn
}

func New(ctx context.Context, t *testing.T, connStr string) (*Conn, error) {
	cfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("%w parse config", err)
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: postgres new", err)
	}

	dbname, err := createTempDb(ctx, conn)
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

func createTempDb(ctx context.Context, conn *pgx.Conn) (string, error) {
	dbname := genDbName()
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

	params := CreateTableOptions{}
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
			return err
		}

		for i := range params.partPolicy.Cnt {
			part := fmt.Sprintf(
				"create table %s_part_%d partition of %s for values with (modulus %d, remainder %d)",
				table.Name.String(), i, table.Name.String(), params.partPolicy.Cnt, i,
			)

			if _, err := c.conn.Exec(ctx, part); err != nil {
				return err
			}
		}

		return nil
	} else {
		_, err := c.conn.Exec(ctx, query)
		return err
	}
}

func (c *Conn) ensureUnexistence(ctx context.Context, name model.TableName) error {
	_, err := c.conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	return err
}

func genDbName() string {
	return xrand.LowerCaseString(10)
}
