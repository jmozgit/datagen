package config

import "github.com/alecthomas/units"

type ConnectionType string

const (
	PostgresqlConnection ConnectionType = "postgresql"
)

type Config struct {
	Version    int        `yaml:"version" koanf:"version"`
	Connection Connection `yaml:"connection" koanf:"connection"`
	Targets    []Target   `yaml:"targets" koanf:"targets"`
	Options    Options    `yaml:"options" koanf:"options"`
}

type Connection struct {
	Type       ConnectionType `yaml:"type" koanf:"type"`
	Postgresql *SQLConnection `yaml:"postgresql" koanf:"posgtresql"`
}

type Target struct {
	Table *Table `yaml:"table" koanf:"table"`
}

type Options struct {
	BatchSize int
}

type Table struct {
	Schema     string           `yaml:"schema" koanf:"schema"`
	Table      string           `yaml:"table" koanf:"table"`
	LimitRows  uint64           `yaml:"limitRows" koanf:"limit_rows"`
	LimitBytes units.Base2Bytes `yaml:"limitBytes" koanf:"limit_bytes"`
	Generators []Generator      `yaml:"generators" koanf:"generators"`
}

type Generator struct {
	Column  string   `yaml:"column" koanf:"column"`
	Type    string   `yaml:"type" koanf:"type"`
	Integer *Integer `yaml:"integer" koanf:"integer"`
}

type Integer struct {
	Format   *string `yaml:"format" koanf:"format"`
	BitSize  *int8   `yaml:"bitSize" koanf:"bit_size"`
	MinValue *int64  `yaml:"minValue" koanf:"min_value"`
	MaxValue *uint64 `yaml:"maxValue" koanf:"max_value"`
}

func defaultKoanfConfigValues() map[string]any {
	return map[string]any{}
}
