package model

import (
	"fmt"
)

type TableName struct {
	Schema Identifier
	Table  Identifier
}

func (t TableName) String() string {
	return fmt.Sprintf("%s.%s", t.Schema.AsArgument(), t.Table.AsArgument())
}

func (t TableName) Quoted() string {
	return fmt.Sprintf("%s.%s", t.Schema.Quoted(), t.Table.Quoted())
}

type Column struct {
	Name       Identifier
	IsNullable bool
	Type       string
	FixedSize  int
}

type Table struct {
	Name          TableName
	Columns       []Column
	UniqueIndexes [][]Identifier
}
