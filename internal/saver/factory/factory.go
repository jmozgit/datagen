package factory

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/saver/postgres"
)

var ErrUnknownConnectionType = errors.New("unknown connection type")

type Saver interface {
	Save(ctx context.Context, batch model.SaveBatch) (model.SaveReport, error)

	// It's here because I don't know how to cover this case better
	SaveAllDefaultValues(
		ctx context.Context,
		schema model.DatasetSchema,
		rows int,
	) (model.SaveReport, error)
}

func GetSaver(ctx context.Context, cfg config.Config) (Saver, error) {
	switch cfg.Connection.Type {
	case config.PostgresqlConnection:
		pgdb, err := postgres.New(ctx, cfg.Connection.Postgresql.ConnString("postgresql"))
		if err != nil {
			return nil, fmt.Errorf("%w: get saver for postgresql", err)
		}

		return pgdb, nil
	default:
		return nil, fmt.Errorf("%w: get saver %s", ErrUnknownConnectionType, cfg.Connection.Type)
	}
}
