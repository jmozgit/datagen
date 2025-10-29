package e2e

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/tests/suite"
	"github.com/stretchr/testify/require"
)

func Test_PostgresqlBytea(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	bs := suite.NewBaseSuite(t)
	table := bs.NewTable("bytea_test", []suite.Column{
		suite.NewColumnRawType("bytea", "bytea"),
	})
	bs.CreateTable(table)

	bs.SaveConfig(
		suite.WithBatchSize(8),
		suite.WithTableTarget(
			config.Table{
				Schema:     table.Schema,
				Table:      table.Name,
				LimitRows:  21,
				LimitBytes: 0,
				Generators: make([]config.Generator, 0),
			},
		),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 1)
		v := toSliceOfByte(t, row[0])
		require.True(t, len(v) <= int(datasize.KB*120))
		cnt++
	})

	require.GreaterOrEqual(t, cnt, 21)
}

func Test_ByteaUserSetttings(t *testing.T) {
	bs := suite.NewBaseSuite(t)
	table := bs.NewTable("bytea_user", []suite.Column{
		suite.NewColumn("bytea", suite.TypeBytea),
	})
	bs.CreateTable(table)

	bs.SaveConfig(
		suite.WithBatchSize(1),
		suite.WithTableTarget(
			config.Table{
				Schema:     table.Schema,
				Table:      table.Name,
				LimitRows:  17,
				LimitBytes: 0,
				Generators: []config.Generator{
					{
						Column: "bytea",
						Type:   config.GeneratorTypeBytea,
						Bytea: &config.LO{
							Size:  datasize.KB * 10,
							Range: datasize.B * 10,
						},
					},
				},
			},
		),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 1)
		v := toSliceOfByte(t, row[0])
		dMin := int(datasize.KB*10 - datasize.B*10)
		dMax := int(datasize.KB*10 + datasize.B*10)
		require.True(t, dMin <= len(v) && len(v) <= dMax)
		cnt++
	})
	require.GreaterOrEqual(t, cnt, 17)
}

func toSliceOfByte(t *testing.T, a any) []byte {
	t.Helper()

	switch v := a.(type) {
	case []byte:
		return v
	default:
		t.Errorf("mismatch []byte, got %T", a)
	}

	return nil
}
