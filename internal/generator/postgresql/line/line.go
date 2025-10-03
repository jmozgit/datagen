package line

import (
	"context"
	"math"
	"math/rand"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/viktorkomarov/datagen/internal/model"
)

type generator struct{}

func NewPostgresql() model.Generator {
	return generator{}
}

func (g generator) Gen(_ context.Context) (any, error) {
	return pgtype.Line{
		A:     math.Float64frombits(rand.Uint64()),
		B:     math.Float64frombits(rand.Uint64()),
		C:     math.Float64frombits(rand.Uint64()),
		Valid: true,
	}, nil
}
