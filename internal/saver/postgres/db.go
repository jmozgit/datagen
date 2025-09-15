package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
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

func (d *DB) Save(ctx context.Context, schema model.DatasetSchema, data [][]any) (model.SaveReport, error) {
	name, err := model.TableNameFromIdentifier(schema.ID)
	if err != nil {
		return model.SaveReport{}, fmt.Errorf("%w: save", err)
	}
	columns := lo.Map(schema.DataTypes, func(taskeType model.TargetType, _ int) string {
		return string(taskeType.SourceName)
	})

	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return model.SaveReport{}, fmt.Errorf("%w: save", err)
	}
	defer conn.Release()

	return save(ctx, conn, pgx.Identifier{string(name.Schema), string(name.Table)}, columns, data)
}

// do it once and more optimal
func insertQuery(table pgx.Identifier, columns []string) string {
	values := strings.Join(lo.Map(columns, func(_ string, idx int) string {
		return fmt.Sprintf("$%d", idx+1)
	}), ",")

	return "INSERT INTO " + table.Sanitize() + fmt.Sprintf(" (%s) VALUES (%s)", strings.Join(columns, ","), values)
}

// it's weird logic, need to proof that it's an optimal way
func save(ctx context.Context, conn *pgxpool.Conn, table pgx.Identifier, columns []string, data [][]any) (model.SaveReport, error) {
	if len(data) < 5 {
		return insert(ctx, conn, table, columns, data)
	}

	_, err := conn.CopyFrom(ctx, table, columns, pgx.CopyFromRows(data))
	if err != nil {
		if IsConstraintViolatesErr(err) {
			mid := len(data) / 2
			leftPart, err := save(ctx, conn, table, columns, data[:mid])
			if err != nil {
				return model.SaveReport{}, err
			}
			rightPart, err := save(ctx, conn, table, columns, data[mid:])
			if err != nil {
				return model.SaveReport{}, err
			}

			return model.SaveReport{
				BytesSaved:          leftPart.BytesSaved + rightPart.BytesSaved,
				RowsSaved:           leftPart.RowsSaved + rightPart.RowsSaved,
				ConstraintViolation: leftPart.ConstraintViolation + rightPart.ConstraintViolation,
			}, nil
		}

		return model.SaveReport{}, err
	}

	return model.SaveReport{RowsSaved: len(data)}, nil
}

func insert(ctx context.Context, conn *pgxpool.Conn, table pgx.Identifier, columns []string, data [][]any) (model.SaveReport, error) {
	collected := model.SaveReport{}
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
