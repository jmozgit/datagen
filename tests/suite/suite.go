package suite

import (
	"os"
	"path"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/config"
	"go.yaml.in/yaml/v3"
)

const configFileName = "config.yaml"

type BaseSuite struct {
	BinPath string
	Config  config.Config
}

type ConfigOption func(cfg *config.Config)

func WithConnection(conn config.Connection) ConfigOption {
	return func(cfg *config.Config) {
		cfg.Connection = conn
	}
}

func WithVersion(version int) ConfigOption {
	return func(cfg *config.Config) {
		cfg.Version = version
	}
}

func WithTableTarget(target config.Target) ConfigOption {
	return func(cfg *config.Config) {
		cfg.Targets = append(cfg.Targets, target)
	}
}

func WithBatchSize(batchSize int) ConfigOption {
	return func(cfg *config.Config) {
		cfg.Options.BatchSize = batchSize
	}
}

func withConnectionOption(t *testing.T) ConfigOption {
	connType, ok := os.LookupEnv("TEST_DATAGEN_CONNECTION_TYPE")
	require.True(t, ok)

	switch connType {
	case "postgresql":
		return postgresqlConnectionOption(t)
	default:
		require.Failf(t, "unknown connection type %s", connType)

		return nil
	}
}

func postgresqlConnectionOption(t *testing.T) ConfigOption {
	connStr, ok := os.LookupEnv("TEST_DATAGEN_PG_CONN")
	require.True(t, ok)

	pgxConf, err := pgx.ParseConfig(connStr)
	require.NoError(t, err)

	return WithConnection(config.Connection{
		Type: "postgresql",
		Postgresql: &config.SQLConnection{
			Host:     pgxConf.Host,
			Port:     int(pgxConf.Port),
			User:     pgxConf.User,
			Password: pgxConf.Password,
			DBName:   pgxConf.Database,
			Options: []string{
				"sslmode=disabled",
			},
		},
	})
}

func NewBaseSuite(t *testing.T) *BaseSuite {
	return &BaseSuite{}
}

func (b *BaseSuite) SaveConfig(t *testing.T, opts ...ConfigOption) {
	t.Helper()

	opts = append(opts, withConnectionOption(t))

	var cfg config.Config
	for _, opt := range opts {
		opt(&cfg)
	}

	savedConfigPath := path.Join(b.BinPath, configFileName)

	data, err := yaml.Marshal(savedConfigPath)
	require.NoError(t, err)

	err = os.WriteFile(savedConfigPath, data, 0o644)
	require.NoError(t, err)
}
