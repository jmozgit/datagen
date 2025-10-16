package reader

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
)

type Connection struct {
	query string
	db    db.Connect
}

func NewConnection(
	tableName model.TableName,
	column model.Identifier,
	limit int,
	db db.Connect,
) *Connection {
	return &Connection{
		query: baseQuery(tableName, column, limit),
		db:    db,
	}
}

func (c *Connection) ReadValues(ctx context.Context) ([]any, error) {
	const fnName = "read values"

	rows, err := c.db.Query(ctx, c.query)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, fnName)
	}
	defer rows.Close()

	values := make([]any, 0)
	for rows.Next() {
		var val any
		if err := rows.Scan(&val); err != nil {
			return nil, fmt.Errorf("%w: %s", err, fnName)
		}

		values = append(values, val)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, fnName)
	}

	return values, nil
}

func baseQuery(
	table model.TableName,
	col model.Identifier,
	batchSize int,
) string {
	// better aproach: see statitistic, if row number is not large, then use ORDER BY RANDOM()
	// use index scan where it's possible
	return fmt.Sprintf(
		`SELECT %s FROM %s TABLESAMPLE BERNOULLI (33) LIMIT %d`,
		col.Quoted(), table.Quoted(), batchSize,
	)
}
