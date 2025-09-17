package suite

import (
	"context"

	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
)

type connection interface {
	CreateTable(ctx context.Context, table model.Table, opts ...options.CreateTableOption) error
	OnEachRow(ctx context.Context, name model.Table, fn func(row []any)) error
}
