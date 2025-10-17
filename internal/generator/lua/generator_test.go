package lua

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_HappyPath(t *testing.T) {
	gen := NewScriptExecutor("testscripts/random.lua")

	val, err := gen.Gen(t.Context())
	require.NoError(t, err)

	str, ok := val.(string)
	require.True(t, ok)

	require.Len(t, str, 20)
}
