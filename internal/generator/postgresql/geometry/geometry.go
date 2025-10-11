package geometry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/viktorkomarov/datagen/internal/model"
)

var ErrUnknownGeometryType = errors.New("unknown geometry type")

func isZero(a float64) bool {
	return a < 1e-9
}

var sourceNameGenerators = map[string]func() any{
	"box": func() any {
		return pgtype.Box{
			P: [2]pgtype.Vec2{
				{
					X: rand.Float64()*180 - 90,
					Y: rand.Float64()*360 - 180,
				},
				{
					X: rand.Float64()*180 - 90,
					Y: rand.Float64()*360 - 180,
				},
			},
			Valid: true,
		}
	},
	"circle": func() any {
		return pgtype.Circle{
			P: pgtype.Vec2{
				X: rand.Float64()*180 - 90,
				Y: rand.Float64()*360 - 180,
			},
			R:     rand.Float64() * 1000,
			Valid: true,
		}
	},
	"line": func() any {
		var (
			a float64
			b float64
		)
		for {
			a, b = math.Float64frombits(rand.Uint64()), math.Float64frombits(rand.Uint64())
			a = math.Round(a*100) / 100
			b = math.Round(b*100) / 100
			if !isZero(a) || !isZero(b) {
				break
			}
		}

		return pgtype.Line{
			A:     a,
			B:     b,
			C:     math.Float64frombits(rand.Uint64()),
			Valid: true,
		}
	},
	"lseg": func() any {
		return pgtype.Lseg{
			P: [2]pgtype.Vec2{
				{
					X: rand.Float64()*180 - 90,
					Y: rand.Float64()*360 - 180,
				},
				{
					X: rand.Float64()*180 - 90,
					Y: rand.Float64()*360 - 180,
				},
			},
			Valid: true,
		}
	},
	"path": func() any {
		size := rand.IntN(30) + 5
		p := make([]pgtype.Vec2, size)
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
		}
	},
	"point": func() any {
		return pgtype.Point{
			P: pgtype.Vec2{
				X: rand.Float64()*180 - 90,
				Y: rand.Float64()*360 - 180,
			},
			Valid: true,
		}
	},
	"polygon": func() any {
		size := rand.IntN(30) + 5
		p := make([]pgtype.Vec2, size)
		for i := range p {
			p[i] = pgtype.Vec2{
				X: rand.Float64()*180 - 90,
				Y: rand.Float64()*360 - 180,
			}
		}

		return pgtype.Polygon{
			P:     p,
			Valid: true,
		}
	},
}

type Generator struct {
	bySourceName map[string]func() any
}

type generator func() any

func (g generator) Gen(_ context.Context) (any, error) {

	val := g()

	fmt.Printf("\n %+v \n", val)

	return val, nil
}

func (g generator) Close() {}

func NewGenerator(sourceName string) (model.Generator, error) {
	gen, ok := sourceNameGenerators[sourceName]
	if !ok {
		return nil, fmt.Errorf("%w: new generator", ErrUnknownGeometryType)
	}

	return generator(gen), nil
}
