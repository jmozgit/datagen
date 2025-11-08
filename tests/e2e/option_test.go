package e2e_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/pkg/db"
	"github.com/jmozgit/datagen/tests/suite"
	"github.com/stretchr/testify/require"
)

func Test_NullFractionOption(t *testing.T) {
	bs := suite.NewBaseSuite(t)

	table := bs.NewTable(
		"null_test",
		[]suite.Column{
			suite.NewColumn("id", suite.TypeSerialInt4),
			suite.NewColumn("might_be_null", suite.TypeText),
		},
	)
	bs.CreateTable(table)

	bs.SaveConfig(
		suite.WithBatchSize(15),
		suite.WithTableTarget(config.Table{
			Schema:    table.Schema,
			Table:     table.Name,
			LimitRows: 100,
			Generators: []config.Generator{
				{
					Column: "might_be_null",
					Type:   config.GeneratorTypeText,
					Text: &config.Text{
						CharLenFrom: 10,
						CharLenTo:   15,
					},
					NullFraction: 25,
				},
			},
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	nullCnt := 0
	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 2)

		if row[1] == nil {
			nullCnt++
		} else {
			_ = toString(t, row[1])
		}
		cnt++
	})

	require.GreaterOrEqual(t, cnt, 100)
	require.True(t, 10 < nullCnt && nullCnt < 40)
}

func Test_NullFractionOptionOnlyNull(t *testing.T) {
	bs := suite.NewBaseSuite(t)

	table := bs.NewTable(
		"null_test_only_null",
		[]suite.Column{
			suite.NewColumn("id", suite.TypeSerialInt4),
			suite.NewColumn("null", suite.TypeText),
		},
	)
	bs.CreateTable(table)

	bs.SaveConfig(
		suite.WithBatchSize(15),
		suite.WithTableTarget(config.Table{
			Schema:    table.Schema,
			Table:     table.Name,
			LimitRows: 100,
			Generators: []config.Generator{
				{Column: "null", NullFraction: 100},
			},
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 2)
		require.Nil(t, row[1])
	})
}

func Test_ReuseValueOptionNoInitValues(t *testing.T) {
	bs := suite.NewBaseSuite(t)

	table := bs.NewTable(
		"reuse_values",
		[]suite.Column{
			suite.NewColumn("id", suite.TypeSerialInt4),
			suite.NewColumn("text", suite.TypeText),
			suite.NewColumn("lo", suite.TypeLO),
		},
	)
	bs.CreateTable(table)

	bs.SaveConfig(
		suite.WithBatchSize(10),
		suite.WithTableTarget(
			config.Table{
				Schema:     table.Schema,
				Table:      table.Name,
				LimitRows:  105,
				LimitBytes: 0,
				Generators: []config.Generator{
					{Column: "text", ReuseFraction: 10},
					{Column: "lo", ReuseFraction: 50},
				},
			},
		),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	textCnt := make(map[string]int)
	loCnt := make(map[int64]int)

	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 3)
		textCnt[toString(t, row[1])]++
		loCnt[toInteger(t, row[2])]++
		cnt++
	})

	require.GreaterOrEqual(t, cnt, 105)
	require.True(t, 75 < len(textCnt) && len(textCnt) < 100)
	require.True(t, 35 < len(loCnt) && len(loCnt) < 65)
}

func Test_ReuseValueOptionWithInitValues(t *testing.T) {
	bs := suite.NewBaseSuite(t)

	table := bs.NewTable(
		"reuse_values",
		[]suite.Column{
			suite.NewColumn("id", suite.TypeInt4),
			suite.NewColumn("num", suite.TypeInt4),
		},
	)
	bs.CreateTable(table)

	bs.ExecuteInFunc(func(ctx context.Context, c db.Connect) error {
		err := c.Execute(ctx,
			"INSERT INTO reuse_values SELECT s.id, s.id FROM generate_series(1, 10) as s(id)",
		)
		if err != nil {
			return fmt.Errorf("%w: execute in func", err)
		}

		return nil
	})

	bs.SaveConfig(
		suite.WithBatchSize(10),
		suite.WithTableTarget(
			config.Table{
				Schema:     table.Schema,
				Table:      table.Name,
				LimitRows:  105,
				LimitBytes: 0,
				Generators: []config.Generator{
					{Column: "num", ReuseFraction: 100},
				},
			},
		),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	intCnt := make(map[int64]int)

	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 2)
		intCnt[toInteger(t, row[1])]++
		cnt++
	})

	require.GreaterOrEqual(t, cnt, 115)
	require.Len(t, intCnt, 10)

	for _, cnt := range intCnt {
		require.GreaterOrEqualf(t, cnt, 1, "%+v", intCnt)
	}
}
