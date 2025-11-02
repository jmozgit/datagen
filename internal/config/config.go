package config

import (
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
)

type ConnectionType string

const (
	PostgresqlConnection ConnectionType = "postgresql"
)

type Config struct {
	Version    int        `yaml:"version"`
	Connection Connection `yaml:"connection"`
	Targets    []Target   `yaml:"targets"`
	Options    Options    `yaml:"options"`
}

type Connection struct {
	Type       ConnectionType `yaml:"type"`
	Postgresql *SQLConnection `yaml:"postgresql"`
}

func (c Connection) ConnString() string {
	switch c.Type {
	case PostgresqlConnection:
		return c.Postgresql.ConnString("postgresql")
	default:
		panic(fmt.Sprintf("unknown connection type %s", c.Type))
	}
}

type Target struct {
	Table *Table `yaml:"table"`
}

type Options struct {
	BatchSize         int           `yaml:"batchSize"`
	CheckSizeDuration time.Duration `yaml:"check_size_duration"`
}

type Table struct {
	Schema     string            `yaml:"schema"`
	Table      string            `yaml:"table"`
	LimitRows  uint64            `yaml:"limitRows"`
	LimitBytes datasize.ByteSize `yaml:"limitBytes"`
	Generators []Generator       `yaml:"generators"`
}

type Generator struct {
	Column          string           `yaml:"column"`
	Type            GeneratorType    `yaml:"type"`
	Integer         *Integer         `yaml:"integer"`
	Float           *Float           `yaml:"float"`
	Timestamp       *Timestamp       `yaml:"timestamp"`
	UUID            *UUID            `yaml:"uuid"`
	Lua             *Lua             `yaml:"lua"`
	ListProbability *ListProbability `yaml:"list_probability"`
	Text            *Text            `yaml:"text"`
	LO              *LO              `yaml:"lo"`
	Bytea           *LO              `yaml:"bytea"`
	Array           *Array           `yaml:"array"`
}

type Integer struct {
	Format   *string `yaml:"format"`
	ByteSize *int8   `yaml:"byteSize"`
	MinValue *int64  `yaml:"minValue"`
	MaxValue *int64  `yaml:"maxValue"`
}

type Float struct {
	ByteSize *int8 `yaml:"byteSize"`
}

type Timestamp struct {
	OnlyNow bool       `yaml:"onlyNow"`
	From    *time.Time `yaml:"from"`
	To      *time.Time `yaml:"to"`
}

type UUID struct {
	Version *string `yaml:"version"`
}

type Lua struct {
	Path string `yaml:"path"`
}

type ListProbability struct {
	Values       []any `yaml:"values"`
	Distribution []int `yaml:"distribution"`
}

type Text struct {
	CharLenFrom int `yaml:"char_to_from"`
	CharLenTo   int `yaml:"char_len_to"`
}

type LO struct {
	Size  datasize.ByteSize `yaml:"size"`
	Range datasize.ByteSize `yaml:"range"`
}

type Array struct {
	Rows     uint       `yaml:"rows"`
	Cols     uint       `yaml:"cols"`
	ElemType *Generator `yaml:"elem_type"`
}
