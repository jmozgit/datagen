package timestamp

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/viktorkomarov/datagen/internal/model"
)

type alwaysNow struct{}

func newAlwaysNow() model.Generator {
	return alwaysNow{}
}

func (a alwaysNow) Gen(_ context.Context) (any, error) {
	return time.Now(), nil
}

type inRange struct {
	from time.Time
	to   time.Time
}

func newInRange(from, to time.Time) inRange {
	return inRange{from: from, to: to}
}

func (i inRange) Gen(_ context.Context) (any, error) {
	fromUnix := i.from.Unix()
	toUnix := i.to.Unix()

	sec := fromUnix + rand.Int64N(toUnix-fromUnix) //nolint:gosec // ok

	return time.Unix(sec, 0), nil
}
