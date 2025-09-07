package postgres_test

import (
	"os"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/inspector/postgres"
	"github.com/viktorkomarov/datagen/internal/model"
	testpg "github.com/viktorkomarov/datagen/pkg/testconn/postgres"
)

type pgInspectorTestSetup struct {
	testConn *testpg.Conn
	connect  *postgres.Connect
}

type Table struct {
	Name        string
	Columns     [][2]string
	PK          []string
	Constraints [][]string
}

func (t Table) UniqueConstraints() []model.UniqueConstraints {
	fromConstraints := lo.Map(t.Constraints, func(c []string, _ int) model.UniqueConstraints {
		group := lo.Map(c, func(s string, _ int) model.Identifier {
			return model.Identifier(s)
		})
		return model.UniqueConstraints(group)
	})

	pk := lo.Map(t.PK, func(g string, _ int) model.Identifier {
		return model.Identifier(g)
	})

	return append(fromConstraints, model.UniqueConstraints(pk))
}

func toColumns(columns [][2]string) []model.Column {
	return lo.Map(columns, func(c [2]string, _ int) model.Column {
		return model.Column{
			Name:       model.Identifier(c[0]),
			Type:       c[1],
			IsNullable: true,
		}
	})
}

func newPgInspectorTestSetup(t *testing.T, table Table) *pgInspectorTestSetup {
	connStr := os.Getenv("TEST_DATAGEN_PG_CONN")
	if connStr == "" {
		t.Skipf("test pg env host isn't set")
	}

	conn, err := testpg.New(t.Context(), t, connStr)
	require.NoError(t, err)
	require.NoError(t, conn.CreateTable(t.Context(), table.Name, table.Columns, table.PK))

	return &pgInspectorTestSetup{
		testConn: conn,
		connect:  postgres.New(conn.Raw()),
	}
}

func Test_PrimaryKeyMustBeSeen(t *testing.T) {
	t.Parallel()

	table := Table{
		Name: "test_1",
		Columns: [][2]string{
			{"foo", "integer"},
			{"bat", "text"},
			{"created_at", "timestamptz"},
		},
		PK:          []string{"foo"},
		Constraints: [][]string{},
	}

	setup := newPgInspectorTestSetup(t, table)

	name := model.TableName{
		Schema: "public",
		Table:  model.Identifier(table.Name),
	}

	actual, err := setup.connect.Table(t.Context(), name)
	require.NoError(t, err)
	tableEqual(t, name, table, actual)
}

func Test_UnknownTable(t *testing.T) {
	t.Parallel()
}

func Test_NoColumns(t *testing.T) {
	t.Parallel()
}

func Test_NoConstraints(t *testing.T) {
	t.Parallel()
}

func Test_OverlappingConstraints(t *testing.T) {
	t.Parallel()
}

func tableEqual(t *testing.T, name model.TableName, expected Table, actual model.Table) {
	require.Equal(t, name, actual)
	require.ElementsMatch(t, toColumns(expected.Columns), actual.Columns)
	require.ElementsMatch(t, expected.UniqueConstraints(), actual.UniqueConstraints)
}
