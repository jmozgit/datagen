package point

import (
	"context"
	"math/rand/v2"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/viktorkomarov/datagen/internal/model"
)

type generator struct{}

func NewPostgresql() model.Generator {
	return generator{}
}

func (g generator) Gen(_ context.Context) (any, error) {
	return pgtype.Point{
		P: pgtype.Vec2{
			X: rand.Float64()*180 - 90,
			Y: rand.Float64()*360 - 180,
		},
		Valid: true,
	}, nil
}
