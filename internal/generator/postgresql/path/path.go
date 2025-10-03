package path

import (
	"context"
	"math/rand/v2"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/viktorkomarov/datagen/internal/model"
)

type generator struct {
	size int
}

func NewPostgresql(size int) model.Generator {
	return generator{size: size}
}

func (g generator) Gen(_ context.Context) (any, error) {
	p := make([]pgtype.Vec2, rand.IntN(g.size))
	for i := range p {
		p[i] = pgtype.Vec2{
			X: rand.Float64()*180 - 90,
			Y: rand.Float64()*360 - 180,
		}
	}

	return pgtype.Path{
		P:      p,
		Closed: rand.IntN(2) == 0,
		Valid:  true,
	}, nil
}

func (g generator) Close() {}
