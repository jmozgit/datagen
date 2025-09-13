package config

import "github.com/alecthomas/units"

type Config struct {
	Version    int        `yaml:"version" koanf:"version"`
	Connection Connection `yaml:"connection" koanf:"connection"`
	Targets    []Target   `yaml:"targerts" koanf:"targets"`
}

type Connection struct {
	Type       string         `yaml:"type" koanf:"type"`
	Postgresql *SQLConnection `yaml:"postgresql" koanf:"posgtresql"`
}

type SQLConnection struct {
	Host     string   `yaml:"host" koanf:"host"`
	Port     int      `yaml:"port" koanf:"port"`
	User     string   `yaml:"user" koanf:"user"`
	Password string   `yaml:"password" koanf:"password"`
	Options  []string `yaml:"options" koanf:"options"`
}

type Target struct {
	Table *Table `yaml:"table" koanf:"table"`
}

type Table struct {
	Schema     string           `yaml:"schema" koanf:"schema"`
	Table      string           `yaml:"table" koanf:"table"`
	LimitRows  uint64           `yaml:"limit_rows" koanf:"limit_rows"`
	LimitBytes units.Base2Bytes `yaml:"limit_bytes" koanf:"limit_bytes"`
	Generators []Generator      `yaml:"generators" koanf:"generators"`
}

type Generator struct {
	Column  string   `yaml:"column" koanf:"column"`
	Type    string   `yaml:"type" koanf:"type"`
	Integer *Integer `yaml:"integer" koanf:"integer"`
}

type Integer struct {
	Format   *string `yaml:"format" koanf:"format"`
	BitSize  *int8   `yaml:"bit_size" koanf:"bit_size"`
	MinValue *int64  `yaml:"min_value" koanf:"min_value"`
	MaxValue *uint64 `yaml:"max_value" koanf:"max_value"`
}

func defaultKoanfConfigValues() map[string]any {
	return map[string]any{}
}
