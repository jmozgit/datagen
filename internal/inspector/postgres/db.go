package postgres

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/samber/lo"
	"github.com/viktorkomarov/datagen/internal/inspector"
	"github.com/viktorkomarov/datagen/internal/model"
)

type Connect struct {
	conn *pgx.Conn
}

func New(conn *pgx.Conn) *Connect {
	return &Connect{conn: conn}
}

func (c *Connect) Table(ctx context.Context, name model.TableName) (model.Table, error) {
	exists, err := c.doesTableExist(ctx, name)
	if err != nil {
		return model.Table{}, fmt.Errorf("%w: table %s", err, name)
	}

	if !exists {
		return model.Table{}, fmt.Errorf("%w: table %s", inspector.ErrEntityNotFound, err)
	}

	columns, err := c.selectTableColumns(ctx, name)
	if err != nil {
		return model.Table{}, fmt.Errorf("%w: table %s", err, name)
	}

	constraints, err := c.selectUniqueConstraints(ctx, name)
	if err != nil {
		return model.Table{}, fmt.Errorf("%w: table %s", err, name)
	}

	return model.Table{
		Name:              name,
		Columns:           columns,
		UniqueConstraints: constraints,
	}, nil
}

func (c *Connect) doesTableExist(ctx context.Context, name model.TableName) (bool, error) {
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
	if err := c.conn.QueryRow(ctx, query, name.Schema, name.Table).Scan(&exists); err != nil {
		return false, fmt.Errorf("%w: does table exist", err)
	}

	return exists, nil
}

func (c *Connect) selectTableColumns(ctx context.Context, name model.TableName) ([]model.Column, error) {
	const query = `
		SELECT 
			column_name, is_nullable, udt_name
		FROM 
			information_schema.columns
		WHERE
			table_schema = $1 AND table_name = $2
		ORDER BY
			column_name
	`

	type Column struct {
		ColumnName string `db:"column_name"`
		IsNullable string `db:"is_nullable"`
		UdtName    string `db:"udt_name"`
	}

	var columns []Column
	if err := pgxscan.Select(ctx, c.conn, &columns, query, name.Schema, name.Table); err != nil {
		return nil, fmt.Errorf("%w: selectTableColumns", err)
	}

	return lo.Map(columns, func(c Column, _ int) model.Column {
		return model.Column{
			Name:       model.Identifier(c.ColumnName),
			IsNullable: c.IsNullable == "YES",
			Type:       c.UdtName,
		}
	}), nil
}

func (c *Connect) selectUniqueConstraints(ctx context.Context, name model.TableName) ([]model.UniqueConstraints, error) {
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
		ColumnName     string `db:"column_name"`
		ConstraintName string `db:"index_name"`
	}

	var cols []Pair
	if err := pgxscan.Select(ctx, c.conn, &cols, query, name.Schema, name.Table); err != nil {
		return nil, fmt.Errorf("%w: selectUniqueConstraints", err)
	}

	groups := slices.Collect(maps.Values(lo.GroupBy(cols, func(p Pair) string {
		return p.ConstraintName
	})))

	return lo.Map(groups, func(group []Pair, _ int) model.UniqueConstraints {
		columns := lo.Map(group, func(p Pair, _ int) model.Identifier {
			return model.Identifier(p.ColumnName)
		})

		return model.UniqueConstraints(columns)
	}), nil
}
