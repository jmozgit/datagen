package model

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

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

var (
	DiscardedRow []any = nil
)

type SaveBatch struct {
	Schema      DatasetSchema
	Data        [][]any
	SavingHints *SavingHints
}

type SavedBatch struct {
	Stat  SaveReport
	Batch SaveBatch
}

var (
	ErrMissingKey    = errors.New("key is missing")
	ErrIncorrectType = errors.New("type is incorrect")
)

type SavingHints struct {
	hints map[string]any
}

func (s *SavingHints) GetString(key string) (string, error) {
	raw, ok := s.hints[key]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrMissingKey, key)
	}

	str, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%w: expected string, not %T", ErrMissingKey, raw)
	}

	return str, nil
}
