package progress

import (
	"context"
	"maps"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/viktorkomarov/datagen/internal/model"
)

type State struct {
	ActualRows           int64
	TotalRows            int64
	ActualSize           datasize.ByteSize
	TotalSize            datasize.ByteSize
	ViolationConstraints int64
}

func (s State) add(r model.ProgressState) State {
	return State{
		ActualRows:           r.RowsCollected,
		TotalRows:            s.TotalRows,
		ActualSize:           r.SizeCollected,
		TotalSize:            s.TotalSize,
		ViolationConstraints: r.ViolationConstraints,
	}
}

type StateDrawer interface {
	Draw(ctx context.Context, options FlushOptions, states map[string]State) error
}

type Controller struct {
	tables map[string]State
	drawer StateDrawer
	states chan model.ProgressState
}

func NewController(drawer StateDrawer, taskCnt int) *Controller {
	return &Controller{
		tables: make(map[string]State),
		drawer: drawer,
		states: make(chan model.ProgressState, taskCnt),
	}
}

func (c *Controller) RegisterTask(table string, row int64, size datasize.ByteSize) {
	c.tables[table] = State{
		ActualRows:           0,
		ActualSize:           0,
		TotalRows:            row,
		TotalSize:            size,
		ViolationConstraints: 0,
	}
}

func (c *Controller) Collect(ctx context.Context, progress model.ProgressState) {
	select {
	case <-ctx.Done():
		return
	case c.states <- progress:
	}
}

func (c *Controller) Close() {
	close(c.states)
}

func (c *Controller) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			c.flush(ctx, withLastFlush())

			return
		case state := <-c.states:
			collected := c.tables[state.Table]
			c.tables[state.Table] = collected.add(state)
		case <-ticker.C:
			c.flush(ctx)
		}
	}
}

type FlushOptions struct {
	LastFlush bool
}

type flushOption func(fl *FlushOptions)

func withLastFlush() flushOption {
	return func(fl *FlushOptions) {
		fl.LastFlush = true
	}
}

func (c *Controller) flush(ctx context.Context, opt ...flushOption) {
	opts := FlushOptions{
		LastFlush: false,
	}
	for _, o := range opt {
		o(&opts)
	}

	c.drawer.Draw(ctx, opts, maps.Clone(c.tables))
}
