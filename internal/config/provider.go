package config

import (
	"fmt"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

func Load(path string) (Config, error) {
	k := koanf.New(".")

	if err := k.Load(confmap.Provider(defaultKoanfConfigValues(), "."), nil); err != nil {
		return Config{}, fmt.Errorf("%w: default provider", err)
	}

	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return Config{}, fmt.Errorf("%w: yaml provider", err)
	}

	var conf Config
	if err := k.Unmarshal("", &conf); err != nil {
		return Config{}, fmt.Errorf("%w, unmarshal", err)
	}

	return conf, nil
}
