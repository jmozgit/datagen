package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/viktorkomarov/datagen/internal/model"
)

type DB struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, connStr string) (*DB, error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("%w: new", err)
	}

	return &DB{
		pool: pool,
	}, nil
}

func (d *DB) RunSave(ctx context.Context, schema model.DatasetSchema) error {
	_, err := model.TableNameFromIdentifier(schema.ID)
	if err != nil {
		return fmt.Errorf("%w: save", err)
	}

	/*
		first copy
		then split to figure out where is broken row
	*/

	return nil
}
