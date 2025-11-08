package reference_test

import (
	"os"
	"testing"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/reference"
	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/model"
	pgadapter "github.com/jmozgit/datagen/internal/pkg/db/adapter/pgx"
	"github.com/jmozgit/datagen/internal/pkg/testconn/options"
	"github.com/jmozgit/datagen/internal/pkg/testconn/postgres"
	"github.com/jmozgit/datagen/internal/refresolver"
	"github.com/samber/mo"
	"github.com/stretchr/testify/require"
)

type refSuite struct {
	pgConn *postgres.Conn
}

func newTableName(schema, table string) model.TableName {
	return model.TableName{
		Schema: model.PGIdentifier(schema),
		Table:  model.PGIdentifier(table),
	}
}

func (r *refSuite) createBaseTable(t *testing.T) {
	t.Helper()

	err := r.pgConn.CreateTable(
		t.Context(),
		model.Table{
			Name: model.TableName{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("base")},
			Columns: []model.Column{
				{Name: model.PGIdentifier("id"), Type: "int"},
			},
		},
		options.WithPKs([]string{"id"}),
	)
	require.NoError(t, err)
}

func (r *refSuite) insertIntoBaseTable(t *testing.T) []int64 {
	t.Helper()

	var values []int64
	err := pgxscan.Select(
		t.Context(), r.pgConn.Raw(),
		&values,
		"INSERT INTO public.base SELECT * FROM generate_series(1, 100) RETURNING id",
	)
	require.NoError(t, err)

	return values
}

func (r *refSuite) getAcceptRequest() contract.AcceptRequest {
	return contract.AcceptRequest{
		Dataset: model.DatasetSchema{
			TableName: newTableName("public", "child"),
		},
		UserSettings: mo.None[config.Generator](),
		BaseType: mo.Some(model.TargetType{
			SourceName: model.PGIdentifier("base_id"),
			Type:       model.Reference,
			SourceType: "int",
			IsNullable: false,
			FixedSize:  4,
		}),
	}
}

func newRefSuite(
	t *testing.T,
) *refSuite {
	t.Helper()

	connStr := os.Getenv("TEST_DATAGEN_PG_CONN")
	if connStr == "" {
		t.Skipf("test pg env host isn't set")
	}

	conn, err := postgres.New(t, connStr)
	require.NoError(t, err)

	return &refSuite{pgConn: conn}
}

func Test_HeapTable(t *testing.T) {
	testConn := newRefSuite(t)

	testConn.createBaseTable(t)
	values := testConn.insertIntoBaseTable(t)

	// create heap child table
	err := testConn.pgConn.CreateTable(
		t.Context(),
		model.Table{
			Name: newTableName("public", "child"),
			Columns: []model.Column{
				{
					Name: model.PGIdentifier("base_id"),
					Type: "int references base(id)",
				},
			},
		},
	)
	require.NoError(t, err)

	adapter := pgadapter.NewAdapterConn(testConn.pgConn.Raw())
	provider := reference.NewProvider(adapter, refresolver.NewService())

	gen, err := provider.Accept(t.Context(), testConn.getAcceptRequest())
	require.NoError(t, err)

	for i := 0; i < len(values)*3; i++ {
		val, err := gen.Generator.Gen(t.Context())
		require.NoError(t, err)
		valInt, ok := val.(int32)
		require.True(t, ok)

		require.Contains(t, values, int64(valInt))
	}
}

func Test_TableWithPK(t *testing.T) {
	testConn := newRefSuite(t)

	testConn.createBaseTable(t)
	values := testConn.insertIntoBaseTable(t)

	// create heap child table
	err := testConn.pgConn.CreateTable(
		t.Context(),
		model.Table{
			Name: newTableName("public", "child"),
			Columns: []model.Column{
				{Name: model.PGIdentifier("id"), Type: "int"},
				{Name: model.PGIdentifier("base_id"), Type: "int references base(id)"},
			},
		},
		options.WithPKs([]string{"id"}),
	)
	require.NoError(t, err)

	adapter := pgadapter.NewAdapterConn(testConn.pgConn.Raw())
	provider := reference.NewProvider(adapter, refresolver.NewService())

	gen, err := provider.Accept(t.Context(), testConn.getAcceptRequest())
	require.NoError(t, err)

	for i := 0; i < len(values)*3; i++ {
		val, err := gen.Generator.Gen(t.Context())
		require.NoError(t, err)
		valInt, ok := val.(int32)
		require.True(t, ok)

		require.Contains(t, values, int64(valInt))
	}
}
