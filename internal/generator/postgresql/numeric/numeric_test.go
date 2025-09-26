package numeric

import (
	"fmt"
	"os"
	"testing"

	"github.com/samber/mo"
	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	testpg "github.com/viktorkomarov/datagen/internal/pkg/testconn/postgres"
)

type pgNumericTestSetup struct {
	testConn *testpg.Conn
}

func newPgNumericTestSetup(
	t *testing.T,
	table *model.Table,
	opts ...options.CreateTableOption,
) *pgNumericTestSetup {
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

	return &pgNumericTestSetup{testConn: conn}
}

func Test_PositiveScale(t *testing.T) {
	t.Parallel()

	scale := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	precision := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	for _, p := range precision {
		for _, s := range scale {
			setup := newPgNumericTestSetup(t, &model.Table{
				Name: model.TableName{
					Schema: "public",
					Table:  "test_numeric",
				},
				Columns: []model.Column{
					{Name: "gen_col", Type: fmt.Sprintf("NUMERIC(%d, %d)", p, s)},
					{Name: "rev_gen_col", Type: fmt.Sprintf("NUMERIC(%d, %d)", s, p)},
				},
			})

			provider := NewProvider(setup.testConn.Raw())
			gen1, err := provider.Accept(
				t.Context(),
				model.DatasetSchema{
					ID:        "public.test_numeric",
					DataTypes: nil,
				},
				mo.None[config.Generator](),
				mo.Some(model.TargetType{
					SourceName: "gen_col",
					Type:       model.Float,
					SourceType: "numeric",
				}),
			)
			require.NoError(t, err)

			gen2, err := provider.Accept(
				t.Context(),
				model.DatasetSchema{
					ID:        "public.test_numeric",
					DataTypes: nil,
				},
				mo.None[config.Generator](),
				mo.Some(model.TargetType{
					SourceName: "rev_gen_col",
					Type:       model.Float,
					SourceType: "numeric",
				}),
			)
			require.NoError(t, err)

			for range 20 {
				val1, err := gen1.Generator.Gen(t.Context())
				require.NoError(t, err)

				val2, err := gen2.Generator.Gen(t.Context())
				require.NoError(t, err)

				_, err = setup.testConn.Raw().Exec(t.Context(), "INSERT INTO test_numeric (gen_col, rev_gen_col) VALUES ($1, $2)", val1, val2)
				require.NoErrorf(t, err, "case NUMERIC(%d, %d) %v or NUMERIC(%d,%d) %v", p, s, val1, s, p, val2)
			}
		}
	}
}
