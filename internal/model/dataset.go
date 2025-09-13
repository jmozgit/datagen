package model

import "context"

type Identifier string

type DatasetSchema struct {
	ID                Identifier
	DataTypes         []TargetType
	UniqueConstraints []UniqueConstraints
}

type TaskGenerators struct {
	Task
	Generators []Generator
}

type Generator interface {
	Gen(ctx context.Context) (any, error)
}
