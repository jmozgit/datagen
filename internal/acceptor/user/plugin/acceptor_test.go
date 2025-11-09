package plugin

import (
	"os"
	"testing"

	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/samber/mo"
	"github.com/stretchr/testify/require"
)

func Test_PluginAcceptor(t *testing.T) {
	t.Parallel()

	const path = "testdata/testplugin.so"

	if _, err := os.Stat(path); err != nil {
		if os.IsExist(err) {
			require.NoError(t, err)
		}

		t.Skip("testplugin.so isn't build")
	}

	req := contract.AcceptRequest{
		Dataset: model.DatasetSchema{},
		UserSettings: mo.Some(config.Generator{
			Column: "",
			Type:   config.GeneratorTypePlugin,
			Plugin: &config.Plugin{
				Path: path,
			},
		}),
		BaseType:      mo.None[model.TargetType](),
		BaseGenerator: mo.None[model.Generator](),
	}

	provider := NewProvider()
	dec, err := provider.Accept(t.Context(), req)
	require.NoError(t, err)

	gen := dec.Generator

	val := 1
	for i := range 10 {
		actual, err := gen.Gen(t.Context())
		require.NoError(t, err)
		require.Equal(t, val+i, actual.(int))
	}
}
