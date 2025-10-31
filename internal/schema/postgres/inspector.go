package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/schema"

	"github.com/jackc/pgx/v5"
)

type Inspector struct {
	connect *connect
}

func NewInspector(conn *config.SQLConnection) (*Inspector, error) {
	pgxConf, err := pgx.ParseConfig(conn.ConnString("postgresql"))
	if err != nil {
		return nil, fmt.Errorf("%w: new inspector", err)
	}

	return &Inspector{connect: newConnect(pgxConf)}, nil
}

//nolint:gochecknoglobals // more convenient that constants here
var pgRegistryTypes = map[string]model.CommonType{
	"int2": model.Integer, "int4": model.Integer, "int8": model.Integer,
	"numeric": model.Float, "float4": model.Float, "float8": model.Float,
	"timestamp": model.Timestamp, "timestamptz": model.Timestamp, "date": model.Date,
	"text": model.Text, "varchar": model.Text, "bpchar": model.Text,
	"uuid": model.UUID,
}

func getTypeOrDefault(_tp string) model.CommonType {
	tp, ok := pgRegistryTypes[_tp]
	if !ok {
		return model.DriverSpecified
	}

	return tp
}

func (i *Inspector) TableIdentifier(ctx context.Context, table *config.Table) (model.TableName, error) {
	const fnName = "table identifier"

	matchedTables, err := i.connect.ResolveTableNames(ctx, table.Table, table.Schema)
	if err != nil {
		return model.TableName{}, fmt.Errorf("%w: %s", err, fnName)
	}

	switch {
	case len(matchedTables) == 0:
		return model.TableName{}, fmt.Errorf(
			"%w: %s schema: %s table: %s",
			schema.ErrEntityNotFound, fnName,
			table.Schema, table.Table,
		)
	case len(matchedTables) > 1:
		return model.TableName{}, fmt.Errorf(
			"%w: %s try to specify schema for %s",
			schema.ErrTooManyTablesMatched, fnName, table.Table,
		)
	default:
		return matchedTables[0], nil
	}
}

func (i *Inspector) ColumnIdentifier(ctx context.Context, tableName model.TableName, column string) (model.Identifier, error) {
	const fnName = "column identifier"

	columns, err := i.connect.ResolveColumnNames(ctx, tableName, column)
	if err != nil {
		return model.Identifier{}, fmt.Errorf("%w: %s", err, fnName)
	}

	switch {
	case len(columns) == 0:
		return model.Identifier{}, fmt.Errorf(
			"%w: %s schema: %s table: %s column: %s",
			schema.ErrEntityNotFound, fnName,
			tableName.Schema.AsArgument(), tableName.Table.AsArgument(), column,
		)
	case len(columns) > 1:
		return model.Identifier{}, fmt.Errorf(
			"%w: %s schema: %s table: %s column: %s",
			schema.ErrTooManyColumnsMatched, fnName,
			tableName.Schema.AsArgument(), tableName.Table.AsArgument(), column,
		)
	default:
		return columns[0], nil
	}
}

func (i *Inspector) Table(ctx context.Context, name model.TableName) (model.DatasetSchema, error) {
	const fnName = "table"

	table, err := i.connect.Table(ctx, name)
	if err != nil {
		return model.DatasetSchema{}, fmt.Errorf("%w: %s", err, fnName)
	}

	dataTypes := make([]model.TargetType, len(table.Columns))
	for i, col := range table.Columns {
		var arrInfo model.ArrayInfo

		tp, ok := pgRegistryTypes[col.Type]
		if !ok {
			if strings.HasPrefix(col.Type, "_") {
				tp = model.Array
				arrInfo = model.ArrayInfo{
					ElemType:   getTypeOrDefault(col.Type[1:]),
					SourceType: col.Type[1:],
				}
			} else {
				tp = model.DriverSpecified
			}
		}

		dataTypes[i] = model.TargetType{
			SourceName: col.Name,
			SourceType: col.Type,
			Type:       tp,
			IsNullable: col.IsNullable,
			FixedSize:  col.FixedSize,
			ArrayElem:  arrInfo,
		}
	}

	return model.DatasetSchema{
		TableName:         name,
		Columns:           dataTypes,
		UniqueConstraints: table.UniqueIndexes,
	}, nil
}
