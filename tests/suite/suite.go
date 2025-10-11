package suite

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"testing"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/postgres"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

const (
	configFileName       = "config.yaml"
	datagenBin           = "datagen"
	testLogsPath         = "../testlogs"
	postgresqlConnection = "postgresql"
)

type BaseSuite struct {
	t              *testing.T
	conn           connection
	connOption     ConfigOption
	binPath        string
	workPath       string
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

func postgresqlConnectionOption(t *testing.T, conn tempConnAdapter) ConfigOption {
	t.Helper()

	return withConnection(config.Connection{
		Type:       postgresqlConnection,
		Postgresql: conn.SQLConnection(),
	})
}

func NewBaseSuite(t *testing.T) *BaseSuite {
	t.Helper()

	workPath, err := filepath.Abs(filepath.Join(testLogsPath, t.Name()))
	require.NoError(t, err)

	err = os.MkdirAll(workPath, os.ModePerm)
	require.NoError(t, err)

	connType := curConnType(t)
	switch connType {
	case postgresqlConnection:
		connStr, ok := os.LookupEnv("TEST_DATAGEN_PG_CONN")
		require.True(t, ok)

		conn, err := postgres.New(t, connStr)
		require.NoError(t, err)

		return &BaseSuite{
			t:              t,
			conn:           &TypeResolver{tempConnAdapter: conn, connType: postgresqlConnection},
			connOption:     postgresqlConnectionOption(t, conn),
			Config:         config.Config{}, //nolint:exhaustruct // ok
			ConnectionType: connType,
			workPath:       workPath,
			binPath:        binPath(t),
		}
	default:
		require.Failf(t, "unknown connection type %s", connType)

		return nil
	}
}

func (b *BaseSuite) TableName(schema, name string) model.TableName {
	return b.conn.ResolveTableName(
		model.TableName{
			Schema: model.Identifier(schema),
			Table:  model.Identifier(name),
		},
	)
}

func (b *BaseSuite) CreateTable(table Table, opts ...options.CreateTableOption) {
	err := b.conn.CreateTable(b.t.Context(), table, opts...)
	require.NoError(b.t, err)
}

func (b *BaseSuite) OnEachRow(table Table, fn func(row []any), opts ...options.OnEachRowOption) {
	err := b.conn.OnEachRow(b.t.Context(), table, fn, opts...)
	require.NoError(b.t, err)
}

func (b *BaseSuite) ExecuteInFunc(fn func(ctx context.Context, c db.Connect) error) {
	err := b.conn.ExecuteInFunc(b.t.Context(), fn)
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

	data, err := yaml.Marshal(cfg)
	require.NoError(b.t, err)

	err = os.WriteFile(savedConfigPath, data, 0o644) //nolint:gosec,mnd // ok for tests
	require.NoError(b.t, err)
}

func TestOnlyFor(t *testing.T, connType ...string) {
	t.Helper()

	if !slices.Contains(connType, curConnType(t)) {
		t.Skipf("skip test for %s", connType)
	}
}

func (b *BaseSuite) configFileName() string {
	return path.Join(b.workPath, configFileName)
}

func (b *BaseSuite) datagenBin() string {
	return path.Join(b.binPath, datagenBin)
}

type flagsValues struct {
	filePath string
	worker   int
}

type FlagOption func(f *flagsValues)

func WithWorkers(w uint) FlagOption {
	return func(f *flagsValues) {
		f.worker = int(w)
	}
}

func (b *BaseSuite) RunDatagen(ctx context.Context, opts ...FlagOption) error {
	flags := flagsValues{
		filePath: b.configFileName(),
		worker:   -1,
	}

	for _, opt := range opts {
		opt(&flags)
	}

	args := []string{"gen", "-f", b.configFileName()}
	if flags.worker != -1 {
		args = append(args, "-w", fmt.Sprint(flags.worker))
	}

	cmd := exec.CommandContext(ctx, b.datagenBin(), args...) //nolint:gosec // ok for tests

	stdout, err := os.Create(filepath.Join(b.workPath, "stdout"))
	require.NoError(b.t, err)
	defer stdout.Close()

	stderr, err := os.Create(filepath.Join(b.workPath, "stderr"))
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
