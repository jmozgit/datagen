package config

import "github.com/alecthomas/units"

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

type Target struct {
	Table *Table `yaml:"table"`
}

type Options struct {
	BatchSize int `yaml:"batchSize"`
}

type Table struct {
	Schema     string           `yaml:"schema"`
	Table      string           `yaml:"table"`
	LimitRows  uint64           `yaml:"limitRows"`
	LimitBytes units.Base2Bytes `yaml:"limitBytes"`
	Generators []Generator      `yaml:"generators"`
}

type Generator struct {
	Column  string        `yaml:"column"`
	Type    GeneratorType `yaml:"type"`
	Integer *Integer      `yaml:"integer"`
}

type Integer struct {
	Format   *string `yaml:"format"`
	ByteSize *int8   `yaml:"byteSize"`
	MinValue *int64  `yaml:"minValue"`
	MaxValue *int64  `yaml:"maxValue"`
}
