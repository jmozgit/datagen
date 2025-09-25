package postgres

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/schema"

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
}

// it's incorrect, but ok for now.
func (i *Inspector) TargetIdentifier(target config.Target) (model.Identifier, error) {
	return model.Identifier(fmt.Sprintf("%s.%s", target.Table.Schema, target.Table.Table)), nil
}

func (i *Inspector) GeneratorIdentifier(gen config.Generator) (model.Identifier, error) {
	return model.Identifier(gen.Column), nil
}

func (i *Inspector) DataSource(ctx context.Context, id model.Identifier) (model.DatasetSchema, error) {
	name, err := model.TableNameFromIdentifier(id)
	if err != nil {
		return model.DatasetSchema{}, fmt.Errorf("%w: data source", err)
	}

	table, err := i.connect.Table(ctx, name)
	if err != nil {
		return model.DatasetSchema{}, fmt.Errorf("%w: data source", err)
	}

	dataTypes := make([]model.TargetType, len(table.Columns))
	for i, col := range table.Columns {
		tp, ok := pgRegistryTypes[col.Type]
		if !ok {
			return model.DatasetSchema{}, fmt.Errorf("%w: %s (%s) in %s", schema.ErrUnsupportedType, col.Name, col.Type, name)
		}

		dataTypes[i] = model.TargetType{
			SourceName: col.Name,
			SourceType: col.Type,
			Type:       tp,
			IsNullable: col.IsNullable,
			FixedSize:  col.FixedSize,
		}
	}

	return model.DatasetSchema{
		ID:        id,
		DataTypes: dataTypes,
	}, nil
}
