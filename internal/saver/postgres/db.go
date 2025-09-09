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
	name, err := model.TableNameFromIdentifier(schema.ID)
	if err != nil {
		return fmt.Errorf("%w: save", err)
	}

	// only insert row by row to handle constraints
}
