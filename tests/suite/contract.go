package suite

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
)

var ErrUnknownTypeForDriver = errors.New("unknown err for driver")

type connection interface {
	ResolveTableName(name model.TableName) model.TableName
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

const (
	ScemaDefault string = "default"
)

type Type string

const (
	TypeUseRaw     Type = ""
	TypeInt2       Type = "int2"
	TypeInt4       Type = "int4"
	TypeInt8       Type = "int8"
	TypeSerialInt2 Type = "serial2"
	TypeSerialInt4 Type = "serial4"
	TypeSerialInt8 Type = "serial8"
	TypeFloat4     Type = "float4"
	TypeFloat8     Type = "float8"
	TypeTimestamp  Type = "timestamp"
)

type Column struct {
	Name    string
	Type    Type
	RawType string
}

func NewColumnRawType(name, rawType string) Column {
	return Column{
		Name:    name,
		RawType: rawType,
		Type:    TypeUseRaw,
	}
}

func NewColumn(name string, tp Type) Column {
	return Column{
		Name:    name,
		RawType: "",
		Type:    tp,
	}
}

type Table struct {
	Name    model.TableName
	Columns []Column
}

func (c *TypeResolver) ResolveTableName(name model.TableName) model.TableName {
	if string(name.Schema) == ScemaDefault {
		switch c.connType {
		case postgresqlConnection:
			return model.TableName{
				Schema: model.Identifier("public"),
				Table:  name.Table,
			}
		default:
			return name
		}
	}

	return name
}

func (c *TypeResolver) mapColumns(columns []Column) ([]model.Column, error) {
	mappeds := make([]model.Column, 0, len(columns))
	for _, col := range columns {
		//nolint:exhaustruct // it's okay for now
		mapped := model.Column{
			Name: model.Identifier(col.Name),
		}
		if col.Type == TypeUseRaw {
			mapped.Type = col.RawType
		} else {
			switch c.connType {
			case postgresqlConnection:
				c, ok := pgMappgingType[col.Type]
				if !ok {
					return nil, fmt.Errorf("%w: %s", ErrUnknownTypeForDriver, col.Type)
				}
				mapped.Type = c
			default:
				return nil, fmt.Errorf("%w %s", ErrUnknownTypeForDriver, c.connType)
			}
		}

		mappeds = append(mappeds, mapped)
	}

	return mappeds, nil
}

func (c *TypeResolver) CreateTable(ctx context.Context, table Table, opts ...options.CreateTableOption) error {
	columns, err := c.mapColumns(table.Columns)
	if err != nil {
		return fmt.Errorf("%w: create table", err)
	}

	err = c.tempConnAdapter.CreateTable(ctx, model.Table{
		Name:    c.ResolveTableName(table.Name),
		Columns: columns,
	}, opts...)
	if err != nil {
		return fmt.Errorf("%w: create table", err)
	}

	return nil
}

func (c *TypeResolver) OnEachRow(ctx context.Context, table Table, fn func(row []any)) error {
	columns, err := c.mapColumns(table.Columns)
	if err != nil {
		return fmt.Errorf("%w: on each row", err)
	}

	err = c.tempConnAdapter.OnEachRow(ctx, model.Table{
		Name:    c.ResolveTableName(table.Name),
		Columns: columns,
	}, fn)
	if err != nil {
		return fmt.Errorf("%w: on each row", err)
	}

	return nil
}
