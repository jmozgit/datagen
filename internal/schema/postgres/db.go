package postgres

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/schema"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/samber/lo"
)

type connect struct {
	cfg *pgx.ConnConfig
}

func newConnect(cfg *pgx.ConnConfig) *connect {
	return &connect{cfg: cfg}
}

func (c *connect) Table(ctx context.Context, name model.TableName) (model.Table, error) {
	conn, err := pgx.ConnectConfig(ctx, c.cfg)
	if err != nil {
		return model.Table{}, fmt.Errorf("%w: table", err)
	}
	defer conn.Close(ctx)

	exists, err := c.doesTableExist(ctx, conn, name)
	if err != nil {
		return model.Table{}, fmt.Errorf("%w: table %s", err, name)
	}

	if !exists {
		return model.Table{}, fmt.Errorf("%w: table %s", schema.ErrEntityNotFound, name)
	}

	columns, err := c.selectTableColumns(ctx, conn, name)
	if err != nil {
		return model.Table{}, fmt.Errorf("%w: table %s", err, name)
	}

	return model.Table{
		Name:    name,
		Columns: columns,
	}, nil
}

func (c *connect) doesTableExist(
	ctx context.Context,
	conn *pgx.Conn,
	name model.TableName,
) (bool, error) {
	const query = `
		SELECT 
			EXISTS (
   				SELECT FROM 
					information_schema.tables 
   				WHERE  
					table_schema = $1 AND table_name = $2
   			)
	`

	var exists bool
	if err := conn.QueryRow(ctx, query, name.Schema, name.Table).Scan(&exists); err != nil {
		return false, fmt.Errorf("%w: does table exist", err)
	}

	return exists, nil
}

func (c *connect) selectTableColumns(
	ctx context.Context,
	conn *pgx.Conn,
	name model.TableName,
) ([]model.Column, error) {
	const query = `
		SELECT 
			column_name, is_nullable, udt_name, typlen 
		FROM 
			information_schema.columns
		INNER JOIN pg_type
			ON information_schema.columns.udt_name = pg_type.typname
		WHERE
			table_schema = $1 AND table_name = $2
		ORDER BY
			column_name
	`

	type Column struct {
		ColumnName string `db:"column_name"`
		IsNullable string `db:"is_nullable"`
		UdtName    string `db:"udt_name"`
		TypeLen    int    `db:"typlen"` //nolint:tagliatelle // ok here
	}

	var columns []Column
	if err := pgxscan.Select(ctx, conn, &columns, query, name.Schema, name.Table); err != nil {
		return nil, fmt.Errorf("%w: selectTableColumns", err)
	}

	return lo.Map(columns, func(c Column, _ int) model.Column {
		return model.Column{
			Name:       model.Identifier(c.ColumnName),
			IsNullable: c.IsNullable == "YES",
			Type:       c.UdtName,
			FixedSize:  c.TypeLen,
		}
	}), nil
}
