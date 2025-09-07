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
	columns, err := c.selectTableColumns(ctx, name)
	if err != nil {
		return model.Table{}, fmt.Errorf("%w: table %s", err, name)
	}

	if len(columns) == 0 {
		return model.Table{}, fmt.Errorf("%w: table %s", inspector.ErrEmptySchema, name)
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

func (c *Connect) selectTableColumns(ctx context.Context, name model.TableName) ([]model.Column, error) {
	const query = `
		SELECT 
			column_name, is_nullable, udt_name
		FROM 
			information_schema.columns
		WHERE
			table_schema = $1 AND table_name = $2
	`
	schema := safeIdentifier(name.Schema)
	table := safeIdentifier(name.Table)

	type Column struct {
		ColumnName string `db:"column_name"`
		IsNullable string `db:"is_nullable"`
		UdtName    string `db:"udt_name"`
	}

	var columns []Column
	if err := pgxscan.Select(ctx, c.conn, &columns, query, schema, table); err != nil {
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
    		kcu.column_name, tc.constraint_name
		FROM
    		information_schema.table_constraints AS tc
		JOIN 
    		information_schema.key_column_usage AS kcu
		ON 
    		tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		WHERE
    		tc.constraint_type = 'UNIQUE' AND tc.table_schema = $1 AND tc.table_name = $2
	`
	schema := safeIdentifier(name.Schema)
	table := safeIdentifier(name.Table)

	type Pair struct {
		ColumnName     string `db:"column_name"`
		ConstraintName string `db:"constraint_name"`
	}

	var cols []Pair
	if err := pgxscan.Select(ctx, c.conn, &cols, query, schema, table); err != nil {
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

func safeIdentifier(id model.Identifier) string {
	return pgx.Identifier([]string{string(id)}).Sanitize()
}
