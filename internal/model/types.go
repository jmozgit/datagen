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
	Array
)

type ArrayInfo struct {
	ElemType   CommonType
	ElemSize   int64
	SourceType string
}

type TargetType struct {
	SourceName Identifier
	Type       CommonType
	SourceType string
	IsNullable bool
	FixedSize  int
	ArrayElem  ArrayInfo
}

type Subscription func(batch SaveBatch)

type ReferenceResolver interface {
	Register(TableName, TableName, Subscription)
}
