package workmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/model"
)

type waitCtxGenerator struct {
}

func (w waitCtxGenerator) Gen(ctx context.Context) (any, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type notifyWhenStart struct {
	closeCh chan struct{}
}

func (w notifyWhenStart) Gen(ctx context.Context) (any, error) {
	close(w.closeCh)
	return nil, ctx.Err()
}

func Test_ManagerExecuteStopByCtx(t *testing.T) {
	t.Parallel()

	forwardToGeneratorFunc := func(ctx context.Context, task model.TaskGenerators) error {
		_, err := task.Generators[0].Gen(ctx)
		return err
	}

	mng := New(2, forwardToGeneratorFunc)

	executionStarted := make(chan struct{})
	executionFinished := make(chan struct{})
	tasks := []model.TaskGenerators{
		{Generators: []model.Generator{notifyWhenStart{executionStarted}}},
		{Generators: []model.Generator{waitCtxGenerator{}}},
		{Generators: []model.Generator{waitCtxGenerator{}}},
		{Generators: []model.Generator{waitCtxGenerator{}}},
	}

	ctx, cancel := context.WithCancel(t.Context())

	var err error
	go func() {
		defer close(executionFinished)

		err = mng.Execute(ctx, tasks)
	}()

	<-executionStarted
	cancel()
	<-executionFinished

	require.ErrorIs(t, err, context.Canceled)
}
