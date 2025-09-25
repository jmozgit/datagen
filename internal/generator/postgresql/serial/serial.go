package serial

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type seqGenerator struct {
	pool    *pgxpool.Pool
	seqName string
}

// add prefetch.
func (s *seqGenerator) Gen(ctx context.Context) (any, error) {
	var next any
	err := s.pool.QueryRow(ctx, fmt.Sprintf("select nextval(%s)", s.seqName)).Scan(&next)
	if err != nil {
		return nil, fmt.Errorf("%w: serial gen", err)
	}

	return next, nil
}
