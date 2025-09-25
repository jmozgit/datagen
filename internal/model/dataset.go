package model

type Identifier string

type DatasetSchema struct {
	ID        Identifier
	DataTypes []TargetType
}

type TaskGenerators struct {
	Task
	Generators []Generator
}

type SaveBatch struct {
	Schema         DatasetSchema
	ExcludeTargets map[Identifier]struct{}
	Data           [][]any
}
