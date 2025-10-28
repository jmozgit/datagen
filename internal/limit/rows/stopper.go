package rows

import (
	"context"

	"github.com/c2h5oh/datasize"
	"github.com/viktorkomarov/datagen/internal/limit"
	"github.com/viktorkomarov/datagen/internal/model"
)

type Stopper struct {
	rows       int64
	collected  int64
	tableName  string
	errCounter int
	collector  limit.Collector
}

func NewStopper(rows int64, tableName string, collector limit.Collector) *Stopper {
	return &Stopper{
		rows:       rows,
		collector:  collector,
		collected:  0,
		errCounter: 0,
		tableName:  tableName,
	}
}

func (s *Stopper) ContinueAllowed(ctx context.Context, report model.SaveReport) (bool, error) {
	s.collected += int64(report.RowsSaved)
	s.errCounter += report.ConstraintViolation

	s.collector.Collect(ctx, model.ProgressState{
		Table:                s.tableName,
		RowsCollected:        s.collected,
		SizeCollected:        datasize.ByteSize(0),
		ViolationConstraints: int64(s.errCounter),
	})

	return s.collected < s.rows, nil
}
