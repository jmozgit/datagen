package model

import (
	"context"
)

type Identifier string

type DatasetSchema struct {
	ID                Identifier
	DataTypes         []TargetType
	UniqueConstraints []UniqueConstraints
}

type TaskGenerators struct {
	Task
	ExcludeTargets map[Identifier]struct{}
	Generators     []Generator
}

type SaveBatch struct {
	Schema         DatasetSchema
	ExcludeTargets map[Identifier]struct{}
	Data           [][]any
}

type Generator interface {
	Gen(ctx context.Context) (any, error)
}
