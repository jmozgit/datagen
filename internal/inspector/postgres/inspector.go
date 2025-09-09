package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/viktorkomarov/datagen/internal/inspector"
	"github.com/viktorkomarov/datagen/internal/model"
)

type Inspector struct {
	connect *connect
}

func NewInspector(conn *pgx.Conn) *Inspector {
	return &Inspector{
		connect: newConnect(conn),
	}
}

var pgRegistryTypes = map[string]model.CommonType{}

func (i *Inspector) DataSource(ctx context.Context, id model.Identifier) (model.DatasetSchema, error) {
	name, err := model.TableNameFromIdentifier(id)
	if err != nil {
		return model.DatasetSchema{}, fmt.Errorf("%w: data source", err)
	}

	table, err := i.connect.Table(ctx, name)
	if err != nil {
		return model.DatasetSchema{}, fmt.Errorf("%w: data source", err)
	}

	dataTypes := make([]model.BaseType, len(table.Columns))
	for i, col := range table.Columns {
		tp, ok := pgRegistryTypes[col.Type]
		if !ok {
			return model.DatasetSchema{}, fmt.Errorf("%w: %s in %s", inspector.ErrUnsupportedType, col.Name, name)
		}

		dataTypes[i] = model.BaseType{
			SourceName: col.Name,
			SourceType: col.Type,
			Type:       tp,
			IsNullable: col.IsNullable,
		}
	}

	return model.DatasetSchema{
		ID:                id,
		DataTypes:         dataTypes,
		UniqueConstraints: table.UniqueConstraints,
	}, nil
}
