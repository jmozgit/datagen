package model

import "strings"

type Identifier string

func (i Identifier) Unquoted() string {
	return strings.Trim(string(i), `"`)
}

type DatasetSchema struct {
	TableName         TableName
	Columns           []TargetType
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
