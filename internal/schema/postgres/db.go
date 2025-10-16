package postgres

import (
	"context"
	"fmt"
	"maps"
	"slices"

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
		return model.Table{}, fmt.Errorf("%w: table %s", err, name.Quoted())
	}

	if !exists {
		return model.Table{}, fmt.Errorf("%w: table %s", schema.ErrEntityNotFound, name.Quoted())
	}

	columns, err := c.selectTableColumns(ctx, conn, name)
	if err != nil {
		return model.Table{}, fmt.Errorf("%w: table %s", err, name.Quoted())
	}

	uniqueIndexes, err := c.selectUniqueConstraints(ctx, conn, name)
	if err != nil {
		return model.Table{}, fmt.Errorf("%w: table %s", err, name.Quoted())
	}

	return model.Table{
		Name:          name,
		Columns:       columns,
		UniqueIndexes: uniqueIndexes,
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
	if err := conn.QueryRow(ctx, query, name.Schema.AsArgument(), name.Table.AsArgument()).Scan(&exists); err != nil {
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
	if err := pgxscan.Select(ctx, conn, &columns, query, name.Schema.AsArgument(), name.Table.AsArgument()); err != nil {
		return nil, fmt.Errorf("%w: selectTableColumns", err)
	}

	return lo.Map(columns, func(c Column, _ int) model.Column {
		return model.Column{
			Name:       model.PGIdentifier(c.ColumnName),
			IsNullable: c.IsNullable == "YES",
			Type:       c.UdtName,
			FixedSize:  c.TypeLen,
		}
	}), nil
}

func (c *connect) selectUniqueConstraints(
	ctx context.Context,
	conn *pgx.Conn,
	name model.TableName,
) ([][]model.Identifier, error) {
	const query = `
		SELECT
    		i.relname AS index_name,
    		a.attname AS column_name
		FROM 
			pg_class t
		JOIN 
			pg_index ix ON t.oid = ix.indrelid
		JOIN 
			pg_class i ON i.oid = ix.indexrelid
		JOIN 
			pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
		JOIN 
			pg_namespace n ON n.oid = t.relnamespace
		WHERE
			n.nspname = $1
			AND
			t.relname = $2
  			AND 
			ix.indisunique = true
		ORDER BY 
			index_name, column_name
	`
	type Pair struct {
		ColumnName string `db:"column_name"`
		IndexName  string `db:"index_name"`
	}

	var cols []Pair
	if err := pgxscan.Select(ctx, conn, &cols, query, name.Schema.AsArgument(), name.Table.AsArgument()); err != nil {
		return nil, fmt.Errorf("%w: selectUniqueConstraints", err)
	}

	groups := slices.Collect(maps.Values(lo.GroupBy(cols, func(p Pair) string {
		return p.IndexName
	})))

	return lo.Map(groups, func(group []Pair, _ int) []model.Identifier {
		return lo.Map(group, func(p Pair, _ int) model.Identifier {
			return model.PGIdentifier(p.ColumnName)
		})
	}), nil
}

func (c *connect) ResolveTableNames(ctx context.Context, name, schema string) ([]model.TableName, error) {
	const fnName = "resolve table names"
	const queryWithoutSchema = "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE tablename = $1"
	const queryWithSchema = "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE tablename = $1 AND schemaname = $2"

	type Row struct {
		SchemaName string `db:"schemaname"`
		TableName  string `db:"tablename"`
	}

	conn, err := pgx.ConnectConfig(ctx, c.cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: table", err)
	}
	defer conn.Close(ctx)

	var rows []Row

	if schema == "" {
		err = pgxscan.Select(ctx, conn, &rows, queryWithoutSchema, name)
	} else {
		err = pgxscan.Select(ctx, conn, &rows, queryWithSchema, name, schema)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, fnName)
	}

	return lo.Map(rows, func(row Row, _ int) model.TableName {
		return model.TableName{
			Schema: model.PGIdentifier(row.SchemaName),
			Table:  model.PGIdentifier(row.TableName),
		}
	}), nil
}

func (c *connect) ResolveColumnNames(ctx context.Context, name model.TableName, column string) ([]model.Identifier, error) {
	const fnName = "resolve column names"
	const query = "SELECT column_name FROM information_schema.columns WHERE table_name = $1 AND table_schema = $2 AND column_name = $3"

	conn, err := pgx.ConnectConfig(ctx, c.cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: table", err)
	}
	defer conn.Close(ctx)

	var rawColumns []string
	err = pgxscan.Select(
		ctx, conn, &rawColumns,
		query,
		name.Table.AsArgument(), name.Schema.AsArgument(), column,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, fnName)
	}

	return lo.Map(rawColumns, func(row string, _ int) model.Identifier {
		return model.PGIdentifier(row)
	}), nil
}
