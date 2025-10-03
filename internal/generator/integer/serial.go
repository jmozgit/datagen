package integer

import (
	"context"

	"github.com/viktorkomarov/datagen/internal/model"
)

type serialGenerator struct {
	cur int64
}

func NewSerialIntegerGenerator(cur int64) model.Generator {
	return &serialGenerator{cur: cur}
}

func (s *serialGenerator) Gen(_ context.Context) (any, error) {
	cur := s.cur
	s.cur++

	return cur, nil
}

func (s serialGenerator) Close() {}
