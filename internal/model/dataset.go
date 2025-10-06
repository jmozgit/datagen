package model

type Identifier string

type DatasetSchema struct {
	ID                Identifier
	DataTypes         []TargetType
	UniqueConstraints [][]Identifier
}

type TaskGenerators struct {
	Task
	Generators []Generator
}

type SaveBatch struct {
	Schema DatasetSchema
	Data   [][]any
}
