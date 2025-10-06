package taskbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/model"
)

func Test_topSort(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc string
		ids  []model.Identifier
		deps map[model.Identifier][]model.Identifier

		expected    []model.Identifier
		expectedErr bool
	}{
		{
			desc: "top_sort_no_deps",
			ids: []model.Identifier{
				"id", "id1", "id2", "id3",
			},
			deps: make(map[model.Identifier][]model.Identifier),
			expected: []model.Identifier{
				"id", "id1", "id2", "id3",
			},
			expectedErr: false,
		},
		{
			desc: "top_sort_cycled_ref",
			ids: []model.Identifier{
				"id", "id1", "id2", "id3",
			},
			deps: map[model.Identifier][]model.Identifier{
				"id":  {"id1"},
				"id1": {"id"},
			},
			expected:    nil,
			expectedErr: true,
		},
		{
			desc: "top_sort_with_dep",
			ids: []model.Identifier{
				"id", "id1", "id2", "id3", "id4", "id5",
			},
			deps: map[model.Identifier][]model.Identifier{
				"id":  {"id2", "id3"},
				"id2": {"id3"},
				"id4": {"id5"},
				"id5": {"id3"},
			},
			expected:    []model.Identifier{"id3", "id2", "id", "id1", "id5", "id4"},
			expectedErr: false,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			t.Parallel()

			actual, err := topSort(tC.ids, tC.deps)
			require.Equal(t, tC.expectedErr, err != nil, err)
			require.Equal(t, tC.expected, actual)
		})
	}
}
