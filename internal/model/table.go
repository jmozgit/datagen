package model

import "fmt"

type TableName struct {
	Schema Identifier
	Table  Identifier
}

func (t TableName) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}

type Column struct {
	Name       Identifier
	IsNullable bool
	Type       string
}

type UniqueConstraints []Identifier

type Table struct {
	Name    TableName
	Columns []Column
	// TODO::it might be others constraints
	UniqueConstraints []UniqueConstraints
}
