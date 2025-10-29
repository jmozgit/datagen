package bytea

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	gen := NewAroundByteaGenerator(datasize.KB*10, datasize.B*100)

	_, err := gen.Gen(t.Context())
	require.NoError(t, err)
}
