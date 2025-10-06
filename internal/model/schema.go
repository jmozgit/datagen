package model

import (
	"context"

	"github.com/viktorkomarov/datagen/internal/config"
)

type SchemaProvider interface {
	TargetIdentifier(target config.Target) (Identifier, error)
	GeneratorIdentifier(gen config.Generator) (Identifier, error)
	DataSource(ctx context.Context, id Identifier) (DatasetSchema, error)
}

type ValuesReader interface {
	ReadValues(ctx context.Context) ([]any, error)
}
