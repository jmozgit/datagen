package ahead

import (
	"context"
	"database/sql"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

type generatorMockNoError struct{}

func (g generatorMockNoError) Gen(_ context.Context) (any, error) {
	return rand.Int(), nil
}

func (g generatorMockNoError) Close() {}

type generatorReturnError struct{ n int }

func (g *generatorReturnError) Gen(_ context.Context) (any, error) {
	if g.n == 0 {
		return nil, sql.ErrNoRows
	}
	g.n--

	return rand.Int(), nil
}

func (g *generatorReturnError) Close() {}

func Test_AheadNoError(t *testing.T) {
	t.Parallel()

	a := NewGenerator(generatorMockNoError{})
	for i := 0; i < 10; i++ {
		_, err := a.Gen(t.Context())
		require.NoError(t, err)
	}
	a.Close()
}

func Test_WaitInnerGeneratorError(t *testing.T) {
	t.Parallel()

	a := NewGenerator(&generatorReturnError{n: 10})

	cnt := 10
	for {
		_, err := a.Gen(t.Context())
		if err != nil {
			break
		}
		cnt--
	}
	require.Equal(t, 0, cnt)
	a.Close()
}
