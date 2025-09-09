package model

import "fmt"

type CommonType int

const (
	Integer CommonType = iota
	Float
	Text
	Time
	Date
	TimeDate
)

type BaseType struct {
	SourceName Identifier
	Type       CommonType
	SourceType string
	IsNullable bool
}

func (b BaseType) String() string {
	return fmt.Sprintf(
		"BaseType[Type %d, SourceType %s]",
		b.Type, b.SourceType,
	)
}
