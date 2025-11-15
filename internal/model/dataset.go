package model

import (
	"context"
	"errors"
	"fmt"

	"github.com/c2h5oh/datasize"
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

type Limit struct {
	Rows int64
	Size datasize.ByteSize
}

type Ticket struct {
	AllowedRows int64
}

type Limiter interface {
	NextTicket(context.Context, int64) (Ticket, error)
	Collect(context.Context, SaveReport)
}

type Task struct {
	DatasetSchema DatasetSchema
	Generators    []Generator
	Limiter       Limiter
}

func (t *Task) TableName() string {
	return t.DatasetSchema.TableName.String()
}

type SaveBatch struct {
	Schema      DatasetSchema
	Data        [][]any
	SavingHints *SavingHints

	Invalid []bool
}

func (s *SaveBatch) MakeInvalid(idx int) {
	s.Invalid[idx] = true
}

func (s *SaveBatch) IsValid(idx int) bool {
	return !s.Invalid[idx]
}

func (s *SaveBatch) Reset() {
	for i := range s.Invalid {
		s.Invalid[i] = false
	}
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

func NewSavingHints() *SavingHints {
	return &SavingHints{
		hints: make(map[string]any),
	}
}

func (s *SavingHints) AddString(key string, value string) {
	s.hints[key] = value
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
