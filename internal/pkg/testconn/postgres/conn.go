package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/pkg/db"
	pgxadapter "github.com/jmozgit/datagen/internal/pkg/db/adapter/pgx"
	"github.com/jmozgit/datagen/internal/pkg/testconn/options"
	"github.com/jmozgit/datagen/internal/pkg/xrand"

	"github.com/jackc/pgx/v5"
	"github.com/samber/lo"
)

type Conn struct {
	saveSchema bool
	conn       *pgx.Conn
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

	c := &Conn{conn: tempConn, saveSchema: false}

	t.Cleanup(func() {
		ctx := context.Background()
		if err = tempConn.Close(ctx); err != nil {
			t.Errorf("failed to close temp conn: %v", err)
		}

		if !c.saveSchema {
			_, err := conn.Exec(ctx, fmt.Sprintf("DROP DATABASE %s WITH (force)", dbname))
			if err != nil {
				t.Errorf("failed to drop database %s: %v", dbname, err)
			}
		}

		if err = conn.Close(ctx); err != nil {
			t.Errorf("failed to close conn: %v", err)
		}
	})

	return c, nil
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

func (c *Conn) CreateTable(ctx context.Context, table model.Table, opts ...options.CreateTableOption) error {
	if err := c.ensureUnexistence(ctx, table.Name); err != nil {
		return fmt.Errorf("%w: ensure unexistence %s", err, table.Name.Quoted())
	}

	params := options.CreateTableOptions{
		PKs: make([]string, 0),
		PartPolicy: options.PartPolicy{
			Method: "",
			Cnt:    0,
			Field:  "",
		},
		Preserve: false,
	}
	for _, opt := range opts {
		opt(&params)
	}

	if params.Preserve {
		c.saveSchema = true
	}

	query := fmt.Sprintf("create table %s (", table.Name.Quoted())
	query += strings.Join(lo.Map(table.Columns, func(c model.Column, _ int) string {
		return fmt.Sprintf("%s %s", c.Name.Quoted(), c.Type)
	}), ",")

	if len(params.PKs) != 0 {
		query += fmt.Sprintf(",primary key (%s)", strings.Join(params.PKs, ","))
	}

	if len(params.FGs) != 0 {
		query += fmt.Sprintf(", %s", params.FGs)
	}

	query += ")"

	if params.PartPolicy.Method != "" {
		query += fmt.Sprintf("partition by hash(%s)", params.PartPolicy.Field)
		if _, err := c.conn.Exec(ctx, query); err != nil {
			return fmt.Errorf("%w: create table", err)
		}

		for i := range params.PartPolicy.Cnt {
			part := fmt.Sprintf(
				"create table %s_part_%d partition of %s for values with (modulus %d, remainder %d)",
				table.Name.Table.AsArgument(), i, table.Name.Quoted(), params.PartPolicy.Cnt, i,
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

func (c *Conn) OnEachRow(
	ctx context.Context,
	table model.Table,
	fn func(row []any),
	opts ...options.OnEachRowOption,
) error {
	optsRow := options.OnEachRow{
		ScanFn: nil,
	}
	for _, opt := range opts {
		opt(&optsRow)
	}

	columns := lo.Map(table.Columns, func(c model.Column, _ int) string {
		return c.Name.Quoted()
	})
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + table.Name.Quoted()

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("%w: on each row", err)
	}
	defer rows.Close()

	buf := make([]any, len(table.Columns))
	ptrBuf := make([]any, len(buf))
	for i := range ptrBuf {
		ptrBuf[i] = &buf[i]
	}

	for rows.Next() {
		if optsRow.ScanFn != nil {
			buf, err := optsRow.ScanFn(rows)
			if err != nil {
				return fmt.Errorf("%w: on each row", err)
			}

			fn(buf)
		} else {
			if err := rows.Scan(ptrBuf...); err != nil {
				return fmt.Errorf("%w: on each row", err)
			}

			fn(buf)
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("%w: on each row", err)
	}

	return nil
}

func (c *Conn) ExecuteInFunc(ctx context.Context, fn func(ctx context.Context, conn db.Connect) error) error {
	adapter := pgxadapter.NewAdapterConn(c.conn)

	err := fn(ctx, adapter)
	if err != nil {
		return fmt.Errorf("%w: execute in func", err)
	}

	return nil
}

func (c *Conn) SQLConnection() *config.SQLConnection {
	return &config.SQLConnection{
		Host:     c.conn.Config().Host,
		Port:     int(c.conn.Config().Port),
		User:     c.conn.Config().User,
		Password: c.conn.Config().Password,
		DBName:   c.conn.Config().Database,
		Options:  make([]string, 0),
	}
}

func (c *Conn) ensureUnexistence(ctx context.Context, name model.TableName) error {
	_, err := c.conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name.Quoted()))
	if err != nil {
		return fmt.Errorf("%w: ensure unexistence", err)
	}

	return nil
}

func genDBName() string {
	const dbNameLen = 10

	return xrand.LowerCaseString(dbNameLen)
}
