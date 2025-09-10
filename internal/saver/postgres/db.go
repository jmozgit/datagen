package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/viktorkomarov/datagen/internal/model"
)

type Connector struct {
	conn *pgx.Conn
}

func (c *Connector) RunSave(ctx context.Context, schema model.DatasetSchema) error {
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
