package model

import (
	"fmt"
	"strings"
)

type TableName struct {
	Schema Identifier
	Table  Identifier
}

func TableNameFromIdentifier(id Identifier) (TableName, error) {
	split := strings.Split(string(id), ".")
	if len(split) != 2 {
		return TableName{}, fmt.Errorf("invalid table name identifier: %s", id) // make it typed
	}

	return TableName{
		Schema: Identifier(split[0]),
		Table:  Identifier(split[1]),
	}, nil
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
