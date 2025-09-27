package e2e_test

import (
	"testing"
	"time"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/tests/suite"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_PGTimeTypes(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	baseSuite := suite.NewBaseSuite(t)
	table := suite.Table{
		Name: baseSuite.TableName("public", "test_pg_timestampts"),
		Columns: []suite.Column{
			suite.NewColumnRawType("ts", "timestamptz"),
			suite.NewColumnRawType("ts1", "timestamp"),
		},
	}
	baseSuite.CreateTable(table)

	baseSuite.SaveConfig(
		suite.WithBatchSize(3),
		suite.WithTableTarget(config.Table{
			Schema:     string(table.Name.Schema),
			Table:      string(table.Name.Table),
			Generators: []config.Generator{},
			LimitRows:  3,
			LimitBytes: 0,
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	baseSuite.OnEachRow(table, func(row []any) {
		require.Len(t, row, len(table.Columns))
		for _, v := range row {
			_ = toTime(t, v)
		}
		cnt++
	})
	require.Equal(t, 3, cnt)
}

func Test_TimestampFromColumnType(t *testing.T) {
	baseSuite := suite.NewBaseSuite(t)
	table := suite.Table{
		Name:    baseSuite.TableName(suite.ScemaDefault, "test_timestamp_column_type"),
		Columns: []suite.Column{suite.NewColumn("col", suite.TypeTimestamp)},
	}
	baseSuite.CreateTable(table)

	baseSuite.SaveConfig(
		suite.WithBatchSize(1),
		suite.WithTableTarget(config.Table{
			Schema:     string(table.Name.Schema),
			Table:      string(table.Name.Table),
			Generators: []config.Generator{},
			LimitRows:  4,
			LimitBytes: 0,
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	baseSuite.OnEachRow(table, func(row []any) {
		require.Len(t, row, len(table.Columns))
		for _, v := range row {
			_ = toTime(t, v)
		}
		cnt++
	})
	require.Equal(t, 4, cnt)
}

//nolint:funlen // ok for test
func Test_TimestampFromUserSettings(t *testing.T) {
	baseSuite := suite.NewBaseSuite(t)
	table := suite.Table{
		Name: baseSuite.TableName(suite.ScemaDefault, "test_timestamp_user_settings"),
		Columns: []suite.Column{
			suite.NewColumn("time_default", suite.TypeTimestamp),
			suite.NewColumn("time_always_now", suite.TypeTimestamp),
			suite.NewColumn("time_from_to", suite.TypeTimestamp),
			suite.NewColumn("time_from", suite.TypeTimestamp),
		},
	}
	baseSuite.CreateTable(table)

	matchInTimeRange := func(lhs, rhs, val time.Time) bool {
		isBefore := lhs.Before(val)
		isAfter := rhs.After(val)

		return isBefore && isAfter
	}

	formatTime := func(timestamp time.Time) string {
		return timestamp.Format(time.DateTime)
	}

	now := time.Now()
	matchColFn := [4]func(t *testing.T, v time.Time){
		func(t *testing.T, v time.Time) {
			t.Helper()

			assert.False(t, v.IsZero())
		},
		func(t *testing.T, v time.Time) {
			t.Helper()

			before := now.Add(-time.Second * 10)
			after := now.Add(time.Second * 10)
			assert.Truef(t,
				matchInTimeRange(before, after, v),
				"not %s < %s < %s",
				formatTime(before), formatTime(v), formatTime(after),
			)
		},
		func(t *testing.T, v time.Time) {
			t.Helper()

			before := now.Add(-time.Hour)
			after := now.Add(time.Second * 10)
			assert.Truef(t,
				matchInTimeRange(before, after, v),
				"not %s < %s < %s",
				formatTime(before), formatTime(v), formatTime(after),
			)
		},
		func(t *testing.T, v time.Time) {
			t.Helper()

			before := now.Add(time.Hour)
			after := now.Add(time.Hour * 24 * 60)
			assert.Truef(t,
				matchInTimeRange(before, after, v),
				"not %s < %s < %s",
				formatTime(before), formatTime(v), formatTime(after),
			)
		},
	}

	baseSuite.SaveConfig(
		suite.WithBatchSize(6),
		suite.WithTableTarget(config.Table{
			Schema: string(table.Name.Schema),
			Table:  string(table.Name.Table),
			Generators: []config.Generator{
				//nolint:exhaustruct // it's oneof
				{Column: "time_default", Type: config.GeneratorTypeTimestamp},
				//nolint:exhaustruct // it's oneof
				{
					Column: "time_always_now", Type: config.GeneratorTypeTimestamp,
					Timestamp: &config.Timestamp{OnlyNow: true, From: nil, To: nil},
				},
				//nolint:exhaustruct // it's oneof
				{
					Column: "time_from_to", Type: config.GeneratorTypeTimestamp,
					Timestamp: &config.Timestamp{OnlyNow: false, From: lo.ToPtr(now.Add(-time.Hour)), To: lo.ToPtr(now)},
				},
				//nolint:exhaustruct // it's oneof
				{
					Column: "time_from", Type: config.GeneratorTypeTimestamp,
					Timestamp: &config.Timestamp{OnlyNow: false, To: nil, From: lo.ToPtr(now.Add(time.Hour))},
				},
			},
			LimitRows:  4,
			LimitBytes: 0,
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	baseSuite.OnEachRow(table, func(row []any) {
		require.Len(t, row, len(table.Columns))
		for i, col := range row {
			timestamp := toTime(t, col)
			matchColFn[i](t, timestamp)
		}
		cnt++
	})
	require.Equal(t, 4, cnt)
}

func toTime(t *testing.T, val any) time.Time {
	t.Helper()

	switch v := val.(type) {
	case time.Time:
		return v
	default:
		require.Failf(t, "time mismatched", "expected time type, not %T (%v)", val, val)
	}

	return time.Time{}
}
