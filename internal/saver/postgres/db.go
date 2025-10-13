package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/viktorkomarov/datagen/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
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

func (d *DB) Save(ctx context.Context, batch model.SaveBatch) (model.SaveReport, error) {
	schema := batch.Schema
	data := batch.Data

	columns := lo.Map(schema.Columns, func(ct model.TargetType, _ int) string {
		return ct.SourceName.Unquoted()
	})

	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return model.SaveReport{}, fmt.Errorf("%w: save", err)
	}
	defer conn.Release()

	tableName := pgx.Identifier{schema.TableName.Schema.Unquoted(), schema.TableName.Table.Unquoted()}

	return save(ctx, conn, tableName, columns, data)
}

// do it once and more optimal.
func insertQuery(table pgx.Identifier, columns []string) string {
	values := strings.Join(lo.Map(columns, func(_ string, idx int) string {
		return fmt.Sprintf("$%d", idx+1)
	}), ",")

	return "INSERT INTO " + table.Sanitize() + fmt.Sprintf(" (%s) VALUES (%s)", strings.Join(columns, ","), values)
}

// it's weird logic, need to proof that it's an optimal way.
func save(
	ctx context.Context,
	conn *pgxpool.Conn,
	table pgx.Identifier,
	columns []string,
	data [][]any,
) (model.SaveReport, error) {
	const copyThreshold = 5

	if len(data) < copyThreshold {
		return insert(ctx, conn, table, columns, data)
	}

	_, err := conn.CopyFrom(ctx, table, columns, pgx.CopyFromRows(data))
	if err != nil {
		if IsConstraintViolatesErr(err) {
			return splitAndCollect(ctx, conn, table, columns, data)
		}

		return model.SaveReport{}, err //nolint:wrapcheck // it's recursive implementation, fix later
	}

	return model.SaveReport{
		ConstraintViolation: 0,
		BytesSaved:          0,
		RowsSaved:           len(data),
	}, nil
}

func splitAndCollect(
	ctx context.Context,
	conn *pgxpool.Conn,
	table pgx.Identifier,
	columns []string,
	data [][]any,
) (model.SaveReport, error) {
	mid := len(data) / 2 //nolint:mnd // half
	leftPart, err := save(ctx, conn, table, columns, data[:mid])
	if err != nil {
		return model.SaveReport{}, err //nolint:nolintlint,wrapcheck // it's recursive implementation, fix later
	}

	rightPart, err := save(ctx, conn, table, columns, data[mid:])
	if err != nil {
		return model.SaveReport{}, err //nolint:nolintlint,wrapcheck // it's recursive implementation, fix later
	}

	return model.SaveReport{
		BytesSaved:          leftPart.BytesSaved + rightPart.BytesSaved,
		RowsSaved:           leftPart.RowsSaved + rightPart.RowsSaved,
		ConstraintViolation: leftPart.ConstraintViolation + rightPart.ConstraintViolation,
	}, nil
}

func insert(
	ctx context.Context,
	conn *pgxpool.Conn,
	table pgx.Identifier,
	columns []string,
	data [][]any,
) (model.SaveReport, error) {
	collected := model.SaveReport{
		BytesSaved:          0,
		RowsSaved:           0,
		ConstraintViolation: 0,
	}
	query := insertQuery(table, columns)
	for _, row := range data {
		_, err := conn.Exec(ctx, query, row...)
		if err != nil {
			if IsConstraintViolatesErr(err) {
				collected.ConstraintViolation++

				continue
			}

			return model.SaveReport{}, fmt.Errorf("%w: insert", err)
		}
		collected.RowsSaved++
	}

	return collected, nil
}
