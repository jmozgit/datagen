package model

import (
	"fmt"
)

type CommonType int

const (
	DriverSpecified CommonType = iota
	Integer
	Float
	Text
	Timestamp
	Date
	UUID
	Reference
)

type TargetType struct {
	SourceName Identifier
	Type       CommonType
	SourceType string
	IsNullable bool
	FixedSize  int
}

func (b TargetType) String() string {
	return fmt.Sprintf(
		"TargetType[Type %d, SourceType %s]",
		b.Type, b.SourceType,
	)
}

type Subscription func(batch SaveBatch)
