package postgresql

import (
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/numeric"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/serial"
	"github.com/viktorkomarov/datagen/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

func DefaultProviderGenerators(pool *pgxpool.Pool) ([]model.GeneratorProvider, error) {
	return []model.GeneratorProvider{
		serial.NewProvider(pool),
		numeric.NewProvider(pool),
	}, nil
}
