package integer_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/generator/integer"
)

func Test_GeneratorValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc        string
		size        int8
		options     []integer.Option
		assertErrFn func(t *testing.T, err error)
	}{
		{
			desc: "incorrect_format",
			size: 8,
			options: []integer.Option{
				integer.WithFormat(integer.Format(145)),
			},
			assertErrFn: func(t *testing.T, err error) {
				require.ErrorIs(t, err, integer.ErrUnknownFormat)
			},
		},
		{
			desc:    "incorrect_size",
			size:    5,
			options: []integer.Option{},
			assertErrFn: func(t *testing.T, err error) {
				require.ErrorIs(t, err, integer.ErrUnsupportedSize)
			},
		},
		{
			desc: "incorrect_border_for_int8_max",
			size: 8,
			options: []integer.Option{
				integer.WithMaxValue(256),
			},
			assertErrFn: func(t *testing.T, err error) {
				require.ErrorIs(t, err, integer.ErrIncorrectMinMaxValue)
			},
		},
		{
			desc: "incorrect_border_for_int8_min",
			size: 8,
			options: []integer.Option{
				integer.WithMinValue(math.MinInt8 - 1),
			},
			assertErrFn: func(t *testing.T, err error) {
				require.ErrorIs(t, err, integer.ErrIncorrectMinMaxValue)
			},
		},
		{
			desc: "correct_lower_border",
			size: 16,
			options: []integer.Option{
				integer.WithMinValue(20),
				integer.WithMaxValue(math.MaxUint16 - 1),
			},
			assertErrFn: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			desc: "edge_case_uint",
			size: 64,
			options: []integer.Option{
				integer.WithMinValue(0),
				integer.WithMaxValue(math.MaxUint64),
			},
			assertErrFn: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			desc: "edge_case_int",
			size: 32,
			options: []integer.Option{
				integer.WithMinValue(math.MinInt32),
				integer.WithMaxValue(math.MaxInt32),
			},
			assertErrFn: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			t.Parallel()

			_, err := integer.New(tC.size, tC.options...)
			tC.assertErrFn(t, err)
		})
	}
}
