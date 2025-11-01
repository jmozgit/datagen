package postgres //nolint:testpackage // add to make progress

import (
	"database/sql"
	"os"
	"testing"

	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/pkg/testconn/options"
	testpg "github.com/jmozgit/datagen/internal/pkg/testconn/postgres"
	"github.com/jmozgit/datagen/internal/schema"

	"github.com/stretchr/testify/require"
)

type pgInspectorTestSetup struct {
	testConn *testpg.Conn
	connect  *connect
}

func newPgInspectorTestSetup(
	t *testing.T,
	table *model.Table,
	opts ...options.CreateTableOption,
) *pgInspectorTestSetup {
	t.Helper()

	connStr := os.Getenv("TEST_DATAGEN_PG_CONN")
	if connStr == "" {
		t.Skipf("test pg env host isn't set")
	}

	conn, err := testpg.New(t, connStr)
	require.NoError(t, err)

	if table != nil {
		require.NoError(t, conn.CreateTable(t.Context(), *table, opts...))
	}

	return &pgInspectorTestSetup{
		testConn: conn,
		connect:  newConnect(conn.Raw().Config()),
	}
}

func Test_UnknownTable(t *testing.T) {
	t.Parallel()

	setup := newPgInspectorTestSetup(t, nil)
	_, err := setup.connect.Table(t.Context(), model.TableName{
		Schema: model.PGIdentifier("public"),
		Table:  model.PGIdentifier("unknown"),
	})
	require.ErrorIs(t, err, schema.ErrEntityNotFound)
}

func Test_NoColumns(t *testing.T) {
	t.Parallel()

	table := model.Table{
		Name: model.TableName{
			Schema: model.PGIdentifier("public"),
			Table:  model.PGIdentifier("no_columns"),
		},
		Columns: []model.Column{},
	}

	setup := newPgInspectorTestSetup(t, &table)
	actual, err := setup.connect.Table(t.Context(), table.Name)
	require.NoError(t, err)
	tableEqual(t, table, actual)
}

func Test_PartitionParentTable(t *testing.T) {
	t.Parallel()

	table := model.Table{
		Name: model.TableName{
			Schema: model.PGIdentifier("public"),
			Table:  model.PGIdentifier("parent"),
		},
		Columns: []model.Column{
			{Name: model.PGIdentifier("col1"), Type: "int8", IsNullable: false, FixedSize: 8},
			{Name: model.PGIdentifier("col2"), Type: "int8", IsNullable: true, FixedSize: 8},
			{Name: model.PGIdentifier("col3"), Type: "int8", IsNullable: true, FixedSize: 8},
			{
				Name: model.PGIdentifier("col4"), Type: "_int4",
				IsNullable: true, FixedSize: -1,
				ElemSizeByte: sql.NullInt64{Valid: true, Int64: 4},
			},
		},
	}

	setup := newPgInspectorTestSetup(t,
		&table,
		options.WithPKs([]string{"col1"}),
		options.WithHashPartitions(5, "col1"),
	)
	actual, err := setup.connect.Table(t.Context(), table.Name)
	require.NoError(t, err)
	tableEqual(t, table, actual)
}

func Test_PartitionChildTable(t *testing.T) {
	t.Parallel()

	table := model.Table{
		Name: model.TableName{
			Schema: model.PGIdentifier("public"),
			Table:  model.PGIdentifier("child"),
		},
		Columns: []model.Column{
			{Name: model.PGIdentifier("col1"), Type: "int8", IsNullable: false, FixedSize: 8},
			{Name: model.PGIdentifier("col2"), Type: "int8", IsNullable: true, FixedSize: 8},
			{Name: model.PGIdentifier("col3"), Type: "int8", IsNullable: true, FixedSize: 8},
		},
	}

	setup := newPgInspectorTestSetup(t,
		&table,
		options.WithPKs([]string{"col1"}),
		options.WithHashPartitions(5, "col1"),
	)
	table.Name = model.TableName{
		Schema: model.PGIdentifier("public"),
		Table:  model.PGIdentifier("child_part_0"),
	}
	actual, err := setup.connect.Table(t.Context(), table.Name)
	require.NoError(t, err)
	tableEqual(t, table, actual)
}

func tableEqual(t *testing.T, expected, actual model.Table) {
	t.Helper()

	require.Equal(t, expected.Name, actual.Name)
	require.Equal(t, expected.Columns, actual.Columns)
}
