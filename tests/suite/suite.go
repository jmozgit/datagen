package suite

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/postgres"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

const (
	configFileName = "config.yaml"
	datagenBin     = "datagen"
	testLogsPath   = "../testlogs"
)

type BaseSuite struct {
	t              *testing.T
	conn           connection
	connOption     ConfigOption
	binPath        string
	Config         config.Config
	ConnectionType string
}

type ConfigOption func(cfg *config.Config)

func withConnection(conn config.Connection) ConfigOption {
	return func(cfg *config.Config) {
		cfg.Connection = conn
	}
}

func withVersion(version int) ConfigOption {
	return func(cfg *config.Config) {
		cfg.Version = version
	}
}

func WithTableTarget(table config.Table) ConfigOption {
	return func(cfg *config.Config) {
		cfg.Targets = append(cfg.Targets, config.Target{Table: &table})
	}
}

func WithBatchSize(batchSize int) ConfigOption {
	return func(cfg *config.Config) {
		cfg.Options.BatchSize = batchSize
	}
}

func postgresqlConnectionOption(t *testing.T, connStr string) ConfigOption {
	t.Helper()

	pgxConf, err := pgx.ParseConfig(connStr)
	require.NoError(t, err)

	return withConnection(config.Connection{
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
	t.Helper()

	connType := curConnType(t)
	switch connType {
	case "postgresql":
		connStr, ok := os.LookupEnv("TEST_DATAGEN_PG_CONN")
		require.True(t, ok)

		conn, err := postgres.New(t, connStr)
		require.NoError(t, err)

		return &BaseSuite{
			t:              t,
			conn:           conn,
			connOption:     postgresqlConnectionOption(t, connStr),
			Config:         config.Config{}, //nolint:exhaustruct // ok
			ConnectionType: connType,
			binPath:        binPath(t),
		}
	default:
		require.Failf(t, "unknown connection type %s", connType)

		return nil
	}
}

func (b *BaseSuite) CreateTable(table model.Table, opts ...options.CreateTableOption) {
	err := b.conn.CreateTable(b.t.Context(), table, opts...)
	require.NoError(b.t, err)
}

func (b *BaseSuite) OnEachRow(table model.Table, fn func(row []any)) {
	err := b.conn.OnEachRow(b.t.Context(), table, fn)
	require.NoError(b.t, err)
}

func (b *BaseSuite) SaveConfig(opts ...ConfigOption) {
	b.t.Helper()

	opts = append(opts, b.connOption, withVersion(1))

	var cfg config.Config
	for _, opt := range opts {
		opt(&cfg)
	}

	savedConfigPath := b.configFileName()

	data, err := yaml.Marshal(savedConfigPath)
	require.NoError(b.t, err)

	err = os.WriteFile(savedConfigPath, data, 0o644) //nolint:gosec,mnd // ok for tests
	require.NoError(b.t, err)
}

func (b *BaseSuite) configFileName() string {
	return path.Join(b.binPath, configFileName)
}

func (b *BaseSuite) datagenBin() string {
	return path.Join(b.binPath, datagenBin)
}

func (b *BaseSuite) RunDatagen(ctx context.Context) error {
	args := []string{"gen", "-f", b.configFileName()}

	cmd := exec.CommandContext(ctx, b.datagenBin(), args...) //nolint:gosec // ok for tests

	workPath := filepath.Join(testLogsPath, b.t.Name())

	err := os.MkdirAll(workPath, 0o666) //nolint:mnd // ok for tests
	require.NoError(b.t, err)

	stdout, err := os.Create(filepath.Join(workPath, "stdou"))
	require.NoError(b.t, err)
	defer stdout.Close()

	stderr, err := os.Create(filepath.Join(workPath, "stdou"))
	require.NoError(b.t, err)
	defer stderr.Close()

	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err = cmd.Run(); err != nil {
		return fmt.Errorf("%w: datagen", err)
	}

	return nil
}

func curConnType(t *testing.T) string {
	t.Helper()

	connType, ok := os.LookupEnv("TEST_DATAGEN_CONNECTION_TYPE")
	require.True(t, ok)

	return connType
}

func binPath(t *testing.T) string {
	t.Helper()

	bin, ok := os.LookupEnv("TEST_DATAGEN_BIN_PATH")
	require.True(t, ok)

	return bin
}
