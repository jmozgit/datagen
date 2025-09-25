package integer //nolint:testpackage // validation test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:funlen // it's a table test
func Test_GeneratorValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc        string
		size        int8
		options     []Option
		assertErrFn func(t *testing.T, err error)
	}{
		{
			desc: "incorrect_format",
			size: 8,
			options: []Option{
				WithFormat(Format("145")),
			},
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, ErrUnknownFormat)
			},
		},
		{
			desc:    "incorrect_size",
			size:    5,
			options: []Option{},
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, ErrUnsupportedSize)
			},
		},
		{
			desc: "incorrect_border_for_int8_max",
			size: 1,
			options: []Option{
				WithMaxValue(256),
			},
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, ErrIncorrectMinMaxValue)
			},
		},
		{
			desc: "incorrect_border_for_int8_min",
			size: 1,
			options: []Option{
				WithMinValue(math.MinInt8 - 1),
			},
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, ErrIncorrectMinMaxValue)
			},
		},
		{
			desc: "correct_lower_border",
			size: 2,
			options: []Option{
				WithMinValue(20),
				WithMaxValue(math.MaxUint16 - 1),
			},
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
		{
			desc: "edge_case_uint",
			size: 8,
			options: []Option{
				WithMinValue(0),
				WithMaxValue(math.MaxInt64),
			},
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
		{
			desc: "edge_case_int",
			size: 4,
			options: []Option{
				WithMinValue(math.MinInt32),
				WithMaxValue(math.MaxInt32),
			},
			assertErrFn: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			t.Parallel()

			_, err := newGenerator(tC.size, tC.options...)
			tC.assertErrFn(t, err)
		})
	}
}
