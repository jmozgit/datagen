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

type TargetType struct {
	SourceName Identifier
	Type       CommonType
	SourceType string
	FixedSize  int
	IsNullable bool
}

func (b TargetType) String() string {
	return fmt.Sprintf(
		"TargetType[Type %d, SourceType %s]",
		b.Type, b.SourceType,
	)
}
