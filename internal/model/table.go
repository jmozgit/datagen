package model

import (
	"errors"
	"fmt"
	"strings"
)

var ErrIncorrectTableName = errors.New("table name format schema.table")

type TableName struct {
	Schema Identifier
	Table  Identifier
}

// it's incorrect, `.` might be in schema/table name.
func TableNameFromIdentifier(id Identifier) (TableName, error) {
	const sep = 2

	split := strings.Split(string(id), ".")
	if len(split) != sep {
		return TableName{}, fmt.Errorf("%w, invalid table name identifier: %s", ErrIncorrectTableName, id)
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
	FixedSize  int
}

type Table struct {
	Name    TableName
	Columns []Column
}
