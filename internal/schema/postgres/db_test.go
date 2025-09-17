package postgres //nolint:testpackage // add to make progress

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	testpg "github.com/viktorkomarov/datagen/internal/pkg/testconn/postgres"
	"github.com/viktorkomarov/datagen/internal/pkg/xrand"
	"github.com/viktorkomarov/datagen/internal/schema"

	"github.com/samber/lo"
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

func (p *pgInspectorTestSetup) createUniqueConstraints(t *testing.T, table model.Table) {
	t.Helper()

	conn := p.testConn.Raw()

	for i := range table.UniqueConstraints {
		constraint := lo.Map(
			table.UniqueConstraints[i],
			func(m model.Identifier, _ int) string {
				return string(m)
			},
		)

		query := fmt.Sprintf(
			"CREATE UNIQUE INDEX %s ON %s (%s)",
			xrand.LowerCaseString(5),
			table.Name.String(),
			strings.Join(constraint, ","),
		)

		_, err := conn.Exec(t.Context(), query)
		require.NoError(t, err)
	}
}

func Test_PrimaryKeyMustBeSeen(t *testing.T) {
	t.Parallel()

	table := model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "test_1",
		},
		Columns: []model.Column{
			{Name: "bat", Type: "text", IsNullable: true},
			{Name: "created_at", Type: "timestamptz", IsNullable: true},
			{Name: "foo", Type: "int4", IsNullable: false},
		},
		UniqueConstraints: []model.UniqueConstraints{
			{"foo"},
		},
	}

	setup := newPgInspectorTestSetup(t, &table, options.WithPKs([]string{"foo"}))

	actual, err := setup.connect.Table(t.Context(), table.Name)
	require.NoError(t, err)
	tableEqual(t, table, actual)
}

func Test_UnknownTable(t *testing.T) {
	t.Parallel()

	setup := newPgInspectorTestSetup(t, nil)
	_, err := setup.connect.Table(t.Context(), model.TableName{
		Schema: "public",
		Table:  "unknown",
	})
	require.ErrorIs(t, err, schema.ErrEntityNotFound)
}

func Test_NoColumns(t *testing.T) {
	t.Parallel()

	table := model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "no_columns",
		},
		Columns:           []model.Column{},
		UniqueConstraints: []model.UniqueConstraints{},
	}

	setup := newPgInspectorTestSetup(t, &table)
	actual, err := setup.connect.Table(t.Context(), table.Name)
	require.NoError(t, err)
	tableEqual(t, table, actual)
}

func Test_OverlappingConstraints(t *testing.T) {
	t.Parallel()

	table := model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "overlapping_constraints",
		},
		Columns: []model.Column{
			{Name: "col1", Type: "int8", IsNullable: true},
			{Name: "col2", Type: "int8", IsNullable: true},
			{Name: "col3", Type: "int8", IsNullable: true},
			{Name: "col4", Type: "int8", IsNullable: true},
		},
		UniqueConstraints: []model.UniqueConstraints{
			{"col1", "col2"},
			{"col2", "col3", "col4"},
			{"col3"},
			{"col1", "col2", "col3", "col4"},
		},
	}

	setup := newPgInspectorTestSetup(t, &table)
	setup.createUniqueConstraints(t, table)

	actual, err := setup.connect.Table(t.Context(), table.Name)
	require.NoError(t, err)
	tableEqual(t, table, actual)
}

func Test_PartitionParentTable(t *testing.T) {
	t.Parallel()

	table := model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "parent",
		},
		Columns: []model.Column{
			{Name: "col1", Type: "int8", IsNullable: false},
			{Name: "col2", Type: "int8", IsNullable: true},
			{Name: "col3", Type: "int8", IsNullable: true},
		},
		UniqueConstraints: []model.UniqueConstraints{{"col1"}},
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
			Schema: "public",
			Table:  "child",
		},
		Columns: []model.Column{
			{Name: "col1", Type: "int8", IsNullable: false},
			{Name: "col2", Type: "int8", IsNullable: true},
			{Name: "col3", Type: "int8", IsNullable: true},
		},
		UniqueConstraints: []model.UniqueConstraints{{"col1"}},
	}

	setup := newPgInspectorTestSetup(t,
		&table,
		options.WithPKs([]string{"col1"}),
		options.WithHashPartitions(5, "col1"),
	)
	table.Name = model.TableName{
		Schema: "public",
		Table:  "child_part_0",
	}
	actual, err := setup.connect.Table(t.Context(), table.Name)
	require.NoError(t, err)
	tableEqual(t, table, actual)
}

func tableEqual(t *testing.T, expected, actual model.Table) {
	t.Helper()

	require.Equal(t, expected.Name, actual.Name)
	require.Equal(t, expected.Columns, actual.Columns)
	require.ElementsMatch(t, expected.UniqueConstraints, actual.UniqueConstraints)
}
