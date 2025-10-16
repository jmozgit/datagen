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
		ids  []model.TableName
		deps map[model.TableName][]model.TableName

		expected    []model.TableName
		expectedErr bool
	}{
		{
			desc: "top_sort_no_deps",
			ids: []model.TableName{
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id1")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id2")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id3")},
			},
			deps: make(map[model.TableName][]model.TableName),
			expected: []model.TableName{
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id1")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id2")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id3")},
			},
			expectedErr: false,
		},
		{
			desc: "top_sort_cycled_ref",
			ids: []model.TableName{
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id1")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id2")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id3")},
			},
			deps: map[model.TableName][]model.TableName{
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id")}:  {{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id1")}},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id1")}: {{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id")}},
			},
			expected:    nil,
			expectedErr: true,
		},
		{
			desc: "top_sort_with_dep",
			ids: []model.TableName{
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id1")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id2")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id3")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id4")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id5")},
			},
			deps: map[model.TableName][]model.TableName{
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id")}:  {{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id2")}, {Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id3")}},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id2")}: {{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id3")}},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id4")}: {{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id5")}},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id5")}: {{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id3")}},
			},
			expected: []model.TableName{
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id3")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id2")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id1")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id5")},
				{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("id4")},
			},
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
