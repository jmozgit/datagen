package model

import "github.com/jackc/pgx/v5"

type Driver int

const (
	DriverPostgresql Driver = iota
)

type Identifier struct {
	drivder Driver
	value   string
}

func PGIdentifier(val string) Identifier {
	return Identifier{drivder: DriverPostgresql, value: val}
}

func (i Identifier) AsArgument() string {
	return i.value
}

func (i Identifier) Quoted() string {
	switch i.drivder {
	case DriverPostgresql:
		return pgx.Identifier([]string{i.value}).Sanitize()
	default:
		panic("unknown driver")
	}
}

type DatasetSchema struct {
	TableName         TableName
	Columns           []TargetType
	UniqueConstraints [][]Identifier
}

type Stopper interface {
	ContinueAllowed(report SaveReport) (bool, error)
}

type Task struct {
	DatasetSchema DatasetSchema
	Generators    []Generator
	Stopper       Stopper
}

type SaveBatch struct {
	Schema DatasetSchema
	Data   [][]any
}

type SavedBatch struct {
	Stat  SaveReport
	Batch SaveBatch
}
