package model

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

type Subscription func(batch SaveBatch)

type ReferenceResolver interface {
	Register(TableName, TableName, Subscription)
}
