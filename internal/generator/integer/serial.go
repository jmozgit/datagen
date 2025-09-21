package integer

import "context"

type serialGenerator struct {
	cur int64
}

func newSerialGenerator(cur int64) *serialGenerator {
	return &serialGenerator{cur: cur}
}

func (s *serialGenerator) Gen(_ context.Context) (any, error) {
	cur := s.cur
	s.cur++

	return cur, nil
}
