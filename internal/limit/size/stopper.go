package size

import (
	"context"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/viktorkomarov/datagen/internal/limit"
	"github.com/viktorkomarov/datagen/internal/model"
)

type TableSizer interface {
	TableSize(ctx context.Context) (int64, error)
}

type Stopper struct {
	limit     int64
	sizer     TableSizer
	errCnt    int
	tableName string
	initSize  int64
	collector limit.Collector

	mu       sync.Mutex
	curSize  int64
	err      error
	lastSize int64
}

func NewStopper(
	ctx context.Context,
	sizer TableSizer,
	table model.TableName,
	limit int64,
	collector limit.Collector,
	fetchDuration time.Duration,
) *Stopper {
	stopper := &Stopper{
		limit:     limit,
		sizer:     sizer,
		initSize:  -1,
		lastSize:  0,
		errCnt:    0,
		collector: collector,
		tableName: table.String(),
		mu:        sync.Mutex{},
		curSize:   0,
		err:       nil,
	}

	go stopper.run(ctx, fetchDuration)

	return stopper
}

func (s *Stopper) ContinueAllowed(ctx context.Context, report model.SaveReport) (bool, error) {
	s.errCnt += report.ConstraintViolation

	s.mu.Lock()
	stop, err := s.curSize >= s.limit, s.err
	progress := model.ProgressState{
		Table:                s.tableName,
		RowsCollected:        0,
		SizeCollected:        datasize.ByteSize(s.curSize),
		ViolationConstraints: int64(s.errCnt),
	}
	s.mu.Unlock()

	s.collector.Collect(ctx, progress)

	return !stop, err
}

func (s *Stopper) setError(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *Stopper) setCurSize(curSize int64) {
	s.mu.Lock()
	s.curSize = curSize
	s.mu.Unlock()
}

func (s *Stopper) run(ctx context.Context, fetchDuration time.Duration) {
	ticker := time.NewTicker(fetchDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			curSize, err := s.sizer.TableSize(ctx)
			if err != nil {
				s.setError(err)

				return
			}
			s.lastSize = curSize
			if s.initSize == -1 {
				s.initSize = curSize
			} else {
				s.setCurSize(curSize - s.initSize)
			}
		}
	}
}
