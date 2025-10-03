package circle

import (
	"context"
	"math"
	"math/rand/v2"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/viktorkomarov/datagen/internal/model"
)

type generator struct{}

func NewPostgresql() model.Generator {
	return generator{}
}

func (g generator) Gen(_ context.Context) (any, error) {
	return pgtype.Circle{
		P: pgtype.Vec2{
			X: rand.Float64()*180 - 90,
			Y: rand.Float64()*360 - 180,
		},
		R:     math.Float64frombits(rand.Uint64()),
		Valid: true,
	}, nil
}

func (g generator) Close() {}
