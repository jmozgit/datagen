package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/pkg/chans"
	"github.com/jmozgit/datagen/internal/pkg/db"
)

type Stopper struct {
	limit      uint64
	connector  *connector
	calculator *calculator
	wait       sync.WaitGroup
	cancelFn   context.CancelFunc
	values     <-chan []model.LOGenerated

	mu        sync.Mutex
	stickyErr error
}

func NewStopper(
	ctx context.Context,
	limit uint64,
	connect db.Connect,
	tableName model.TableName,
	loGenerated ...<-chan []model.LOGenerated,
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

	neverClose := make(<-chan []model.LOGenerated)

	return &Stopper{
		limit:      limit,
		connector:  connector,
		wait:       sync.WaitGroup{},
		values:     chans.FanIn(append(loGenerated, neverClose)...),
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
		case msg := <-s.values:
			sum := uint64(0)
			for _, s := range msg {
				sum += s.Size
			}
			s.calculator.addDynamicSize(sum)
		}
	}
}
