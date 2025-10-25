package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/saver/common"

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

func (d *DB) PrepareHints(ctx context.Context, schema model.DatasetSchema, generators []model.Generator) *model.SavingHints {
	tableName := pgx.Identifier{schema.TableName.Schema.AsArgument(), schema.TableName.Table.AsArgument()}
	columns := lo.Map(schema.Columns, func(ct model.TargetType, _ int) string {
		return ct.SourceName.AsArgument()
	})
	hints := model.NewSavingHints()
	hints.AddString("insert_query_hint", insertQuery(tableName, columns))

	return hints
}

func (d *DB) Save(ctx context.Context, batch model.SaveBatch) (model.SavedBatch, error) {
	schema := batch.Schema
	tableName := pgx.Identifier{schema.TableName.Schema.AsArgument(), schema.TableName.Table.AsArgument()}
	columns := lo.Map(schema.Columns, func(ct model.TargetType, _ int) string {
		return ct.SourceName.AsArgument()
	})

	report := model.SaveReport{
		RowsSaved:           0,
		ConstraintViolation: 0,
	}

	insQuery, err := batch.SavingHints.GetString("insert_query_hint")
	if err != nil {
		return model.SavedBatch{}, fmt.Errorf("%w: save", err)
	}

	parts := []common.DataPartitionerMut{common.NewDataPartionerMut(batch.Data)}
	for len(parts) > 0 {
		curPart := parts[0]
		parts = parts[1:]

		if curPart.Len() < copyThresholdRowSize {
			saved, err := d.insert(ctx, insQuery, batch, curPart)
			if err != nil {
				return model.SavedBatch{}, fmt.Errorf("%w: save", err)
			}
			report = report.Add(saved)
			continue
		}

		saved, err := d.copy(ctx, tableName, columns, curPart.Data())
		switch {
		case err == nil:
			report = report.Add(saved)
		case IsConstraintViolatesErr(err):
			before, after := curPart.Split()
			parts = append(parts, before, after)
		default:
			return model.SavedBatch{}, fmt.Errorf("%w: save", err)
		}
	}

	return model.SavedBatch{
		Stat:  report,
		Batch: batch,
	}, nil
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
		RowsSaved:           int(rows),
	}, nil
}

func insertQuery(table pgx.Identifier, columns []string) string {
	values := strings.Join(lo.Map(columns, func(_ string, idx int) string {
		return fmt.Sprintf("$%d", idx+1)
	}), ",")

	return "INSERT INTO " + table.Sanitize() + fmt.Sprintf(" (%s) VALUES (%s)", strings.Join(columns, ","), values)
}

func (d *DB) insert(
	ctx context.Context,
	query string,
	batch model.SaveBatch,
	partioner common.DataPartitionerMut,
) (model.SaveReport, error) {
	collected := model.SaveReport{
		RowsSaved:           0,
		ConstraintViolation: 0,
	}

	data := partioner.Data()
	for i, row := range data {
		_, err := d.pool.Exec(ctx, query, row...)
		if err != nil {
			if IsConstraintViolatesErr(err) {
				collected.ConstraintViolation++
				batch.MakeInvalid(partioner.RealIndex(i))
				continue
			}

			return model.SaveReport{}, fmt.Errorf("%w: insert", err)
		}

		collected.RowsSaved++
	}

	return collected, nil
}
