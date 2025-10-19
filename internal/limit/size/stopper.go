package size

import (
	"context"
	"sync"
	"time"

	"github.com/viktorkomarov/datagen/internal/model"
)

type TableSizer interface {
	TableSize(ctx context.Context) (int64, error)
}

type Stopper struct {
	limit int64
	sizer TableSizer

	initSize int64
	mu       sync.Mutex
	stop     bool
	err      error
}

func NewStopper(
	ctx context.Context,
	sizer TableSizer,
	table model.TableName,
	limit int64,
	fetchDuration time.Duration,
) *Stopper {
	stopper := &Stopper{
		limit:    limit,
		sizer:    sizer,
		initSize: -1,
		mu:       sync.Mutex{},
		stop:     false,
		err:      nil,
	}

	go stopper.run(ctx, fetchDuration)

	return stopper
}

func (s *Stopper) ContinueAllowed(_ model.SaveReport) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stop, err := s.stop, s.err

	return !stop, err
}

func (s *Stopper) setError(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *Stopper) setStop(stop bool) {
	s.mu.Lock()
	s.stop = stop
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
			if s.initSize == -1 {
				s.initSize = curSize
			} else {
				s.setStop(curSize-s.initSize > s.limit)
			}
		}
	}
}
