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

	return &Inspector{
		connect: newConnect(pgxConf),
	}, nil
}

//nolint:gochecknoglobals // more convenient that constants here
var pgRegistryTypes = map[string]model.CommonType{}

func (i *Inspector) TargetIdentifier(_ config.Target) (model.Identifier, error) {
	return model.Identifier(""), nil
}

func (i *Inspector) GeneratorIdentifier(_ config.Generator) (model.Identifier, error) {
	return model.Identifier(""), nil
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
			return model.DatasetSchema{}, fmt.Errorf("%w: %s in %s", schema.ErrUnsupportedType, col.Name, name)
		}

		dataTypes[i] = model.TargetType{
			SourceName: col.Name,
			SourceType: col.Type,
			Type:       tp,
			IsNullable: col.IsNullable,
			FixedSize:  -1,
		}
	}

	return model.DatasetSchema{
		ID:                id,
		DataTypes:         dataTypes,
		UniqueConstraints: table.UniqueConstraints,
	}, nil
}
