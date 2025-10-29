package interval

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jmozgit/datagen/internal/model"
)

type generator struct{}

func NewPostgresql() model.Generator {
	return generator{}
}

func (g generator) Gen(_ context.Context) (any, error) {
	return pgtype.Interval{
		Microseconds: rand.Int64N((time.Hour * 24).Microseconds()),
		Days:         rand.Int32N(31),
		Months:       rand.Int32N(12),
		Valid:        true,
	}, nil
}

func (g generator) Close() {}
