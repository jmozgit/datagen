package postgresql

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/viktorkomarov/datagen/internal/model"
)

func DefaultProviderGenerators(pool *pgxpool.Pool) ([]model.GeneratorProvider, error) {
	return []model.GeneratorProvider{}, nil
}
