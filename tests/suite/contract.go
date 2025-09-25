package suite

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
)

type connection interface {
	SQLConnection() *config.SQLConnection
	CreateTable(ctx context.Context, table Table, opts ...options.CreateTableOption) error
	OnEachRow(ctx context.Context, name Table, fn func(row []any)) error
}

type tempConnAdapter interface {
	SQLConnection() *config.SQLConnection
	CreateTable(ctx context.Context, table model.Table, opts ...options.CreateTableOption) error
	OnEachRow(ctx context.Context, name model.Table, fn func(row []any)) error
}

type TypeResolver struct {
	connType string
	tempConnAdapter
}

type Type string

const (
	TypeInt2   Type = "int2"
	TypeInt4   Type = "int4"
	TypeInt8   Type = "int8"
	TypeFloat4 Type = "float4"
	TypeFloat8 Type = "float8"
	UseRaw     Type = "use_raw"
)

type Column struct {
	Name    string
	Type    Type
	RawType string
}

type Table struct {
	Name    model.TableName
	Columns []Column
}

func (c *TypeResolver) CreateTable(ctx context.Context, table Table, opts ...options.CreateTableOption) error {
	columns := make([]model.Column, 0, len(table.Columns))
	for _, col := range table.Columns {
		mapped := model.Column{
			Name: model.Identifier(col.Name),
		}
		if col.Type == UseRaw {
			mapped.Type = col.RawType
		} else {
			switch c.connType {
			case "postgresql":
				c, ok := pgMappgingType[col.Type]
				if !ok {
					return fmt.Errorf("unknown type %s for postgresql", col.Type)
				}
				mapped.Type = c
			default:
				return fmt.Errorf("unsupported mapping %s", c.connType)
			}
		}
	}

	return c.tempConnAdapter.CreateTable(ctx, model.Table{
		Name:    table.Name,
		Columns: columns,
	}, opts...)
}

func (c *TypeResolver) OnEachRow(ctx context.Context, name Table, fn func(row []any)) error {
	return c.tempConnAdapter.OnEachRow(ctx, model.Table{
		Name: name.Name,
	}, fn)
}
