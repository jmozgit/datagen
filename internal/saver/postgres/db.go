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

const copyThresholdRowSize = 10

type DB struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, connStr string) (*DB, error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("%w: new", err)
	}

	return &DB{pool: pool}, nil
}

func (d *DB) Save(ctx context.Context, batch model.SaveBatch) (model.SaveReport, error) {
	schema := batch.Schema
	tableName := pgx.Identifier{schema.TableName.Schema.AsArgument(), schema.TableName.Table.AsArgument()}
	columns := lo.Map(schema.Columns, func(ct model.TargetType, _ int) string {
		return ct.SourceName.AsArgument()
	})

	report := model.SaveReport{
		RowsSaved:           0,
		BytesSaved:          0,
		ConstraintViolation: 0,
	}
	batches := [][][]any{batch.Data}
	for len(batches) > 0 {
		curBatch := batches[0]
		batches = batches[1:]

		if len(curBatch) < copyThresholdRowSize {
			saved, err := d.insert(ctx, tableName, columns, curBatch)
			if err != nil {
				return model.SaveReport{}, fmt.Errorf("%w: save", err)
			}
			report = report.Add(saved)

			continue
		}

		saved, err := d.copy(ctx, tableName, columns, curBatch)
		switch {
		case err == nil:
			report = report.Add(saved)
		case IsConstraintViolatesErr(err):
			mid := len(curBatch) / 2
			batches = append(batches, curBatch[:mid], curBatch[mid:])
		}
	}

	return report, nil
}

func (d *DB) copy(
	ctx context.Context,
	table pgx.Identifier,
	columns []string,
	data [][]any,
) (model.SaveReport, error) {
	rows, err := d.pool.CopyFrom(ctx, table, columns, pgx.CopyFromRows(data))
	if err != nil {
		return model.SaveReport{}, fmt.Errorf("%w: copy", err)
	}

	return model.SaveReport{
		ConstraintViolation: 0,
		BytesSaved:          0,
		RowsSaved:           int(rows),
	}, nil
}

// move it to batch
func insertQuery(table pgx.Identifier, columns []string) string {
	values := strings.Join(lo.Map(columns, func(_ string, idx int) string {
		return fmt.Sprintf("$%d", idx+1)
	}), ",")

	return "INSERT INTO " + table.Sanitize() + fmt.Sprintf(" (%s) VALUES (%s)", strings.Join(columns, ","), values)
}

func (d *DB) insert(
	ctx context.Context,
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
		_, err := d.pool.Exec(ctx, query, row...)
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
