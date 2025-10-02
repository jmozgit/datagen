package serial

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
)

type seqGenerator struct {
	conn    db.Connect
	seqName string
}

func NewSeqBasedGenerator(conn db.Connect, seqName string) model.Generator {
	return &seqGenerator{
		conn:    conn,
		seqName: seqName,
	}
}

// add prefetch.
func (s *seqGenerator) Gen(ctx context.Context) (any, error) {
	var next any

	err := s.conn.QueryRow(ctx, fmt.Sprintf("select nextval('%s')", s.seqName)).Scan(&next)
	if err != nil {
		return nil, fmt.Errorf("%w: serial gen", err)
	}

	return next, nil
}
