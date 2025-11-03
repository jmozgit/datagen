package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/pkg/db"
)

type Stopper struct {
	limit      uint64
	connector  *connector
	calculator *calculator
	wait       sync.WaitGroup
	cancelFn   context.CancelFunc
	values     <-chan model.LOGenerated

	mu        sync.Mutex
	stickyErr error
}

func NewStopper(
	ctx context.Context,
	limit uint64,
	connect db.Connect,
	tableName model.TableName,
	loGenerated ...<-chan model.LOGenerated,
) (*Stopper, error) {
	const fnName = "new stopper"

	connector, err := newConnector(ctx, connect, tableName)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, fnName)
	}

	size, err := connector.TableSize(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, fnName)
	}

	var values <-chan model.LOGenerated
	if len(loGenerated) > 0 {
		values = fanIn(loGenerated...)
	}

	return &Stopper{
		limit:      limit,
		connector:  connector,
		wait:       sync.WaitGroup{},
		values:     values,
		calculator: newCalculator(size),
		cancelFn:   nil,
	}, nil
}

func (s *Stopper) ContinueAllowed(_ context.Context, _ model.SaveReport) (bool, error) {
	generated := s.calculator.collected()

	s.mu.Lock()
	err := s.stickyErr
	s.mu.Unlock()

	return generated < s.limit, err
}

func (s *Stopper) Run(ctx context.Context, fetchPeriod time.Duration) {
	ctx, s.cancelFn = context.WithCancel(ctx)

	s.wait.Add(1)
	go func() {
		defer s.wait.Done()

		s.updateCalculator(ctx, fetchPeriod)
	}()
}

func (s *Stopper) Close() {
	s.cancelFn()
	s.wait.Wait()
}

func (s *Stopper) updateCalculator(
	ctx context.Context,
	fetchUpdate time.Duration,
) {
	timer := time.NewTicker(fetchUpdate)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			size, err := s.connector.TableSize(ctx)
			if err != nil {
				s.mu.Lock()
				s.stickyErr = err
				s.mu.Unlock()
				return
			}

			s.calculator.resetStaticSize(size)
		case msg, ok := <-s.values:
			if !ok {
				s.values = nil // is it ok ??
			}
			s.calculator.addDynamicSize(msg.Size)
		}
	}
}

func fanIn(values ...<-chan model.LOGenerated) <-chan model.LOGenerated {
	out := make(chan model.LOGenerated)

	var wg sync.WaitGroup

	wg.Add(len(values))
	for _, val := range values {
		go func(val <-chan model.LOGenerated) {
			defer wg.Done()

			for v := range val {
				out <- v
			}
		}(val)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
