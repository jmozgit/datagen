package config

import (
	"fmt"
	"strings"
)

type SQLConnection struct {
	Host     string   `yaml:"host" koanf:"host"`
	Port     int      `yaml:"port" koanf:"port"`
	User     string   `yaml:"user" koanf:"user"`
	Password string   `yaml:"password" koanf:"password"`
	DBName   string   `yaml:"dbname" koanf:"dbname"`
	Options  []string `yaml:"options" koanf:"options"`
}

func (s SQLConnection) ConnString(protocol string) string {
	var builder strings.Builder

	builder.WriteString(protocol)
	builder.WriteString("://")
	builder.WriteString(s.User)
	if s.Password != "" {
		builder.WriteRune(':')
		builder.WriteString(s.Password)
	}
	builder.WriteByte('@')
	builder.WriteString(s.Host)
	builder.WriteByte(':')
	builder.WriteString(fmt.Sprint(s.Port))
	builder.WriteByte('/')
	builder.WriteString(s.DBName)
	if len(s.Options) > 0 {
		builder.WriteByte('&')
	}
	builder.WriteString(strings.Join(s.Options, "&"))

	return builder.String()
}
