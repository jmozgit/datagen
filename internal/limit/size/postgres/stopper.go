package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/jmozgit/datagen/internal/limit"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/pkg/chans"
	"github.com/jmozgit/datagen/internal/pkg/db"
)

type Stopper struct {
	limit        uint64
	connector    *connector
	calculator   *calculator
	wait         sync.WaitGroup
	cancelFn     context.CancelFunc
	closeReading chan []model.LOGenerated
	values       <-chan []model.LOGenerated
	tableName    model.TableName
	errCounter   int
	collector    limit.Collector

	mu        sync.Mutex
	stickyErr error
}

func NewStopper(
	ctx context.Context,
	limit uint64,
	connect db.Connect,
	tableName model.TableName,
	collector limit.Collector,
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

	closeReading := make(chan []model.LOGenerated)

	return &Stopper{
		limit:        limit,
		connector:    connector,
		wait:         sync.WaitGroup{},
		values:       chans.FanIn(append(loGenerated, closeReading)...),
		closeReading: closeReading,
		collector:    collector,
		tableName:    tableName,
		calculator:   newCalculator(size),
		cancelFn:     nil,
	}, nil
}

func (s *Stopper) NextTicket(_ context.Context, batchRows int64) (model.Ticket, error) {
	s.mu.Lock()
	err := s.stickyErr
	s.mu.Unlock()

	if err != nil {
		return model.Ticket{}, fmt.Errorf("%w: next ticket", err)
	}

	next := batchRows
	if s.calculator.collected() >= s.limit {
		next = 0
	}

	return model.Ticket{
		AllowedRows: next,
	}, nil
}

func (s *Stopper) Collect(ctx context.Context, report model.SaveReport) {
	s.errCounter += report.ConstraintViolation

	s.collector.Collect(ctx, model.ProgressState{
		Table:                s.tableName.String(),
		RowsCollected:        int64(report.RowsSaved),
		SizeCollected:        datasize.ByteSize(0),
		ViolationConstraints: int64(s.errCounter),
	})
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
	close(s.closeReading)
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
