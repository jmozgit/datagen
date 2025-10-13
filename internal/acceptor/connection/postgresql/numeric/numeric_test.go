package numeric_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/numeric"
	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/db/adapter/pgx"
	testpg "github.com/viktorkomarov/datagen/internal/pkg/testconn/postgres"

	"github.com/samber/mo"
	"github.com/stretchr/testify/require"
)

type pgNumericTestSetup struct {
	testConn *testpg.Conn
}

func newPgNumericTestSetup(
	t *testing.T,
) *pgNumericTestSetup {
	t.Helper()

	connStr := os.Getenv("TEST_DATAGEN_PG_CONN")
	if connStr == "" {
		t.Skipf("test pg env host isn't set")
	}

	conn, err := testpg.New(t, connStr)
	require.NoError(t, err)

	return &pgNumericTestSetup{testConn: conn}
}

func Test_PositiveScale(t *testing.T) {
	t.Parallel()

	scale := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	precision := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	conn := newPgNumericTestSetup(t)

	for _, p := range precision {
		for _, s := range scale {
			err := conn.testConn.CreateTable(t.Context(), model.Table{
				Name: model.TableName{
					Schema: "public",
					Table:  "test_numeric",
				},
				Columns: []model.Column{
					//nolint:exhaustruct // ok
					{Name: "gen_col", Type: fmt.Sprintf("NUMERIC(%d, %d)", p, s)},
					//nolint:exhaustruct // ok
					{Name: "rev_gen_col", Type: fmt.Sprintf("NUMERIC(%d, %d)", s, p)},
				},
			})
			require.NoError(t, err)

			provider := numeric.NewProvider(pgx.NewAdapterConn(conn.testConn.Raw()))

			gen1, err := provider.Accept(
				t.Context(),
				contract.AcceptRequest{
					Dataset: model.DatasetSchema{
						TableName:         model.TableName{Schema: "public", Table: "test_numeric"},
						Columns:           nil,
						UniqueConstraints: nil,
					},
					UserSettings: mo.None[config.Generator](),
					//nolint:exhaustruct // ok
					BaseType: mo.Some(model.TargetType{
						SourceName: "gen_col",
						Type:       model.Float,
						SourceType: "numeric",
					}),
				},
			)
			require.NoError(t, err)

			gen2, err := provider.Accept(
				t.Context(),
				contract.AcceptRequest{
					Dataset: model.DatasetSchema{
						TableName:         model.TableName{Schema: "public", Table: "test_numeric"},
						Columns:           nil,
						UniqueConstraints: nil,
					},
					UserSettings: mo.None[config.Generator](),
					//nolint:exhaustruct // ok
					BaseType: mo.Some(model.TargetType{
						SourceName: "rev_gen_col",
						Type:       model.Float,
						SourceType: "numeric",
					}),
				},
			)
			require.NoError(t, err)

			for range 20 {
				val1, err := gen1.Generator.Gen(t.Context())
				require.NoError(t, err)

				val2, err := gen2.Generator.Gen(t.Context())
				require.NoError(t, err)

				_, err = conn.testConn.Raw().Exec(t.Context(),
					"INSERT INTO test_numeric (gen_col, rev_gen_col) VALUES ($1, $2)", val1, val2,
				)
				require.NoErrorf(t, err, "case NUMERIC(%d, %d) %v or NUMERIC(%d,%d) %v", p, s, val1, s, p, val2)
			}
		}
	}
}

func Test_NegativeScale(t *testing.T) {
	t.Parallel()

	scale := []int{-1, -2, -3, -4, -5, -6, -7, -8, -9, -10}
	precision := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	conn := newPgNumericTestSetup(t)

	for _, p := range precision {
		for _, s := range scale {
			err := conn.testConn.CreateTable(t.Context(), model.Table{
				Name: model.TableName{Schema: "public", Table: "test_negative_scale"},
				//nolint:exhaustruct // ok
				Columns: []model.Column{{Name: "col", Type: fmt.Sprintf("NUMERIC(%d, %d)", p, s)}},
			})
			require.NoError(t, err)

			provider := numeric.NewProvider(pgx.NewAdapterConn(conn.testConn.Raw()))

			gen1, err := provider.Accept(
				t.Context(),
				contract.AcceptRequest{
					Dataset: model.DatasetSchema{
						TableName:         model.TableName{Schema: "public", Table: "test_negative_scale"},
						Columns:           nil,
						UniqueConstraints: nil,
					},
					UserSettings: mo.None[config.Generator](),
					//nolint:exhaustruct // ok
					BaseType: mo.Some(model.TargetType{SourceName: "col", Type: model.Float, SourceType: "numeric"}),
				},
			)
			require.NoError(t, err)

			for range 20 {
				val1, err := gen1.Generator.Gen(t.Context())
				require.NoError(t, err)

				_, err = conn.testConn.Raw().Exec(t.Context(),
					"INSERT INTO test_negative_scale (col) VALUES ($1)", val1,
				)
				require.NoErrorf(t, err, "case NUMERIC(%d, %d) %v", p, s, val1)
			}
		}
	}
}
