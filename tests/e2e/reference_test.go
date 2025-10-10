package e2e_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	"github.com/viktorkomarov/datagen/tests/suite"
)

type referenceSuite struct {
	bs         *suite.BaseSuite
	baseTable  suite.Table
	childTable suite.Table
}

func newReferenceSuite(t *testing.T) referenceSuite {
	t.Helper()

	bs := suite.NewBaseSuite(t)

	baseTable := suite.Table{
		Name: bs.TableName(suite.ScemaDefault, "base_table"),
		Columns: []suite.Column{
			suite.NewColumn("id", suite.TypeInt4),
		},
	}
	bs.CreateTable(baseTable, options.WithPKs([]string{"id"}))

	childTable := suite.Table{
		Name: bs.TableName(suite.ScemaDefault, "child_table"),
		Columns: []suite.Column{
			suite.NewColumn("id", suite.TypeSerialInt4),
			suite.NewColumn("base_id", suite.TypeInt4),
		},
	}
	bs.CreateTable(
		childTable,
		options.WithPKs([]string{"id"}),
		options.WithForeignKey("foreign key (base_id) references base_table(id)"),
	)

	return referenceSuite{
		bs:         bs,
		baseTable:  baseTable,
		childTable: childTable,
	}
}

func (r *referenceSuite) checkBaseAndChildTablesGeneration(
	t *testing.T,
	baseTableCnt, childTableCnt int,
) {
	t.Helper()

	baseTableValues := make(map[int64]bool)
	cnt := 0
	r.bs.OnEachRow(r.baseTable, func(row []any) {
		require.Len(t, row, 1)
		n := toInteger(t, row[0])
		baseTableValues[n] = true
		cnt++
	})
	require.Equal(t, baseTableCnt, cnt)

	cnt = 0
	r.bs.OnEachRow(r.childTable, func(row []any) {
		require.Len(t, row, 2)
		baseID := toInteger(t, row[1])
		_ = toInteger(t, row[0])
		require.True(t, baseTableValues[baseID])
		cnt++
	})

	require.Equal(t, childTableCnt, cnt)
}

func Test_ReferenceSimultaneousGeneration(t *testing.T) {
	refSuite := newReferenceSuite(t)

	refSuite.bs.SaveConfig(
		suite.WithBatchSize(10),
		suite.WithTableTarget(config.Table{
			Schema:     string(refSuite.baseTable.Name.Schema),
			Table:      string(refSuite.baseTable.Name.Table),
			LimitRows:  5,
			LimitBytes: 0,
			Generators: make([]config.Generator, 0),
		}),
		suite.WithTableTarget(config.Table{
			Schema:     string(refSuite.childTable.Name.Schema),
			Table:      string(refSuite.childTable.Name.Table),
			LimitRows:  33,
			LimitBytes: 0,
			Generators: make([]config.Generator, 0),
		}),
	)

	err := refSuite.bs.RunDatagen(t.Context(), suite.WithWorkers(1)) // disable parallel
	require.NoError(t, err)

	refSuite.checkBaseAndChildTablesGeneration(t, 5, 33)
}

func Test_ReferenceParallelGeneration(t *testing.T) {
	refSuite := newReferenceSuite(t)

	refSuite.bs.SaveConfig(
		suite.WithBatchSize(17),
		suite.WithTableTarget(config.Table{
			Schema:     string(refSuite.baseTable.Name.Schema),
			Table:      string(refSuite.baseTable.Name.Table),
			LimitRows:  100,
			LimitBytes: 0,
			Generators: make([]config.Generator, 0),
		}),
		suite.WithTableTarget(config.Table{
			Schema:     string(refSuite.childTable.Name.Schema),
			Table:      string(refSuite.childTable.Name.Table),
			LimitRows:  30,
			LimitBytes: 0,
			Generators: make([]config.Generator, 0),
		}),
	)

	err := refSuite.bs.RunDatagen(t.Context(), suite.WithWorkers(2))
	require.NoError(t, err)

	refSuite.checkBaseAndChildTablesGeneration(t, 100, 30)
}
