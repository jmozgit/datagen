package integer //nolint:testpackage // validation test

import (
	"math"
	"testing"

	"github.com/viktorkomarov/datagen/internal/model"

	"github.com/samber/mo"
	"github.com/stretchr/testify/require"
)

//nolint:funlen // it's a table test
func Test_GeneratorValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc        string
		size        int8
		optBaseType mo.Option[model.TargetType]
		options     []Option
		assertErrFn func(t *testing.T, err error)
	}{
		{
			desc: "incorrect_format",
			size: 8,
			options: []Option{
				WithFormat(Format("145")),
			},
			optBaseType: mo.None[model.TargetType](),
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, ErrUnknownFormat)
			},
		},
		{
			desc:        "incorrect_size",
			size:        5,
			options:     []Option{},
			optBaseType: mo.None[model.TargetType](),
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, ErrUnsupportedSize)
			},
		},
		{
			desc: "incorrect_border_for_int8_max",
			size: 8,
			options: []Option{
				WithMaxValue(256),
			},
			optBaseType: mo.None[model.TargetType](),
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, ErrIncorrectMinMaxValue)
			},
		},
		{
			desc: "incorrect_border_for_int8_min",
			size: 8,
			options: []Option{
				WithMinValue(math.MinInt8 - 1),
			},
			optBaseType: mo.None[model.TargetType](),
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, ErrIncorrectMinMaxValue)
			},
		},
		{
			desc: "correct_lower_border",
			size: 16,
			options: []Option{
				WithMinValue(20),
				WithMaxValue(math.MaxUint16 - 1),
			},
			optBaseType: mo.None[model.TargetType](),
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
		{
			desc: "edge_case_uint",
			size: 64,
			options: []Option{
				WithMinValue(0),
				WithMaxValue(math.MaxInt64),
			},
			optBaseType: mo.None[model.TargetType](),
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
		{
			desc: "edge_case_int",
			size: 32,
			options: []Option{
				WithMinValue(math.MinInt32),
				WithMaxValue(math.MaxInt32),
			},
			optBaseType: mo.None[model.TargetType](),
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			t.Parallel()

			_, err := newGenerator(tC.size, tC.optBaseType, tC.options...)
			tC.assertErrFn(t, err)
		})
	}
}
