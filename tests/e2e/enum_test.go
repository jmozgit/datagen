package e2e_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	"github.com/viktorkomarov/datagen/tests/suite"
)

type pgEnumSuite struct {
	bs         *suite.BaseSuite
	enumValues []string
}

func newPGEnumSuite(t *testing.T) pgEnumSuite {
	t.Helper()

	suite.TestOnlyFor(t, "postgresql")

	bs := suite.NewBaseSuite(t)
	bs.ExecuteInFunc(func(ctx context.Context, c db.Connect) error {
		return c.Execute(ctx, `
		DO $$ BEGIN
    		CREATE TYPE test_enum_type AS enum ('sad', 'ok', 'happy');
		EXCEPTION
    		WHEN duplicate_object THEN null;
		END $$`,
		)
	})

	return pgEnumSuite{
		bs:         bs,
		enumValues: []string{"sad", "ok", "happy"},
	}
}

func (p pgEnumSuite) scanFn(row db.Row) ([]any, error) {
	var (
		id   int
		enum string
	)
	if err := row.Scan(&id, &enum); err != nil {
		return nil, fmt.Errorf("%w: row scan", err)
	}

	return []any{id, enum}, nil
}

func (p pgEnumSuite) createTable() suite.Table {
	table := p.bs.NewTable("enum_table", []suite.Column{
		suite.NewColumn("id", suite.TypeInt2),
		suite.NewColumnRawType("value", "test_enum_type"),
	})
	p.bs.CreateTable(table, options.WithPKs([]string{"id"}))

	return table
}

func Test_PostgresqlEnumValues(t *testing.T) {
	enumSuite := newPGEnumSuite(t)

	table := enumSuite.createTable()
	enumSuite.bs.SaveConfig(
		suite.WithBatchSize(39),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			LimitRows:  73,
			LimitBytes: 0,
			Generators: make([]config.Generator, 0),
		}),
	)

	err := enumSuite.bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	enums := make(map[string]int)
	enumSuite.bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 2)
		val, ok := row[1].(string)
		require.True(t, ok)
		enums[val]++
		cnt++
	}, options.WithScanFn(enumSuite.scanFn))
	require.Equal(t, cnt, 73)
	require.Len(t, enums, 3)

	for _, v := range enumSuite.enumValues {
		require.NotZero(t, enums[v])
	}
}

func Test_EnumUserSettings(t *testing.T) {
	enumSuite := newPGEnumSuite(t)

	table := enumSuite.createTable()

	enumSuite.bs.SaveConfig(
		suite.WithBatchSize(7),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			LimitRows:  19,
			LimitBytes: 0,
			Generators: []config.Generator{
				{
					Column: "value",
					Type:   config.GeneratorTypeProbabilityList,
					ListProbability: &config.ListProbability{
						Values: lo.Map(enumSuite.enumValues, func(v string, _ int) any {
							return v
						}),
						Distribution: []int{30, 30, 30},
					},
				},
			},
		}),
	)

	err := enumSuite.bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	enums := make(map[string]int)
	enumSuite.bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 2)
		val, ok := row[1].(string)
		require.True(t, ok)
		enums[val]++
		cnt++
	}, options.WithScanFn(enumSuite.scanFn))
	require.Equal(t, cnt, 19)
	require.Len(t, enums, 3)

	for _, v := range enumSuite.enumValues {
		require.NotZero(t, enums[v])
	}
}
