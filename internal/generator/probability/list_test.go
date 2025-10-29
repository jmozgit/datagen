package probability_test

import (
	"testing"

	"github.com/jmozgit/datagen/internal/generator/probability"
	"github.com/stretchr/testify/require"
)

func Test_ListProbabilityGenerator(t *testing.T) {
	types := []string{
		"hold", "clearing", "refund",
		"drop", "authorization", "transfer",
	}
	probabilities := []int{
		100, 150, 270,
		400, 80, 160,
	}

	typesExpectedCnt := make(map[string]int)
	for idx, key := range types {
		typesExpectedCnt[key] = probabilities[idx]
	}

	totalCnt := 0
	for _, p := range probabilities {
		totalCnt += p
	}

	generator, err := probability.NewList(probabilities, types)
	require.NoError(t, err)

	stat := make(map[string]int)
	for range totalCnt {
		val, err := generator.Gen(t.Context())
		require.NoError(t, err)

		str, ok := val.(string)
		require.True(t, ok)
		stat[str]++
	}

	for key, actualCnt := range stat {
		expectedCnt := typesExpectedCnt[key]

		minAllowable := int(float64(expectedCnt) * 0.80)
		maxAllowable := int(float64(expectedCnt) * 1.20)

		require.Truef(t, minAllowable < actualCnt && actualCnt < maxAllowable, "stat %v %s", stat, key)
	}
}
