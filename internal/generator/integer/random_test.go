package integer //nolint:testpackage // for devs

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_RandomGenerator(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc string
		minV int64
		maxV int64
	}{
		{desc: "edge_case_int64", minV: math.MinInt64, maxV: math.MaxInt64},
		{desc: "edge_case_int32", minV: math.MinInt32, maxV: math.MaxInt32},
		{desc: "edge_case_int8", minV: math.MinInt8, maxV: math.MaxInt8},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			t.Parallel()

			gen := newRandomGenerator(tC.minV, tC.maxV)
			val, err := gen.Gen(t.Context())
			require.NoError(t, err)

			v, ok := val.(int64)
			require.True(t, ok)
			require.True(t, tC.minV <= v && v <= tC.maxV)
		})
	}
}
