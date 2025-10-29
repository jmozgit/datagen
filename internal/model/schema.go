package model

import (
	"context"

	"github.com/jmozgit/datagen/internal/config"
)

type SchemaProvider interface {
	TableIdentifier(ctx context.Context, table *config.Table) (TableName, error)
	ColumnIdentifier(ctx context.Context, tableName TableName, column string) (Identifier, error)
	Table(ctx context.Context, table TableName) (DatasetSchema, error)
}

type ColumnValueReader interface {
	ReadValues(ctx context.Context) ([]any, error)
}
