package workmanager_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/workmanager"

	"github.com/stretchr/testify/require"
)

type waitCtxGenerator struct{}

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

type returnErr struct {
	err error
}

func (w returnErr) Gen(_ context.Context) (any, error) {
	return nil, w.err
}

type noError struct{}

func (w noError) Gen(_ context.Context) (any, error) {
	return nil, nil //nolint:nilnil // it's ok
}

func Test_ManagerExecuteStopByCtx(t *testing.T) {
	t.Parallel()

	forwardToGeneratorFunc := func(ctx context.Context, task model.TaskGenerators) error {
		_, err := task.Generators[0].Gen(ctx)

		return err
	}

	mng := workmanager.New(2, forwardToGeneratorFunc)

	executionStarted := make(chan struct{})
	executionFinished := make(chan struct{})
	tasks := []model.TaskGenerators{
		//nolint:exhaustruct // ok for tests
		{Generators: []model.Generator{notifyWhenStart{executionStarted}}},
		//nolint:exhaustruct // ok for tests
		{Generators: []model.Generator{waitCtxGenerator{}}},
		//nolint:exhaustruct // ok for tests
		{Generators: []model.Generator{waitCtxGenerator{}}},
		//nolint:exhaustruct // ok for tests
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

var errJob = errors.New("job error")

func Test_ManagerExecuteStopByJobError(t *testing.T) {
	t.Parallel()

	forwardToGeneratorFunc := func(ctx context.Context, task model.TaskGenerators) error {
		_, err := task.Generators[0].Gen(ctx)
		if err != nil {
			return fmt.Errorf("%w: forward", err)
		}

		return err
	}

	mng := workmanager.New(2, forwardToGeneratorFunc)

	executionFinished := make(chan struct{})
	tasks := []model.TaskGenerators{
		//nolint:exhaustruct // it's okay here
		{Generators: []model.Generator{noError{}}},
		//nolint:exhaustruct // it's okay here
		{Generators: []model.Generator{returnErr{errJob}}},
		//nolint:exhaustruct // it's okay here
		{Generators: []model.Generator{waitCtxGenerator{}}},
		//nolint:exhaustruct // it's okay here
		{Generators: []model.Generator{waitCtxGenerator{}}},
	}

	var err error
	go func() {
		defer close(executionFinished)

		err = mng.Execute(t.Context(), tasks)
	}()

	<-executionFinished

	require.ErrorIs(t, err, errJob)
}

func Test_ManagerExecuteNoError(t *testing.T) {
	t.Parallel()

	forwardToGeneratorFunc := func(ctx context.Context, task model.TaskGenerators) error {
		_, err := task.Generators[0].Gen(ctx)
		if err != nil {
			return fmt.Errorf("%w: forward", err)
		}

		return nil
	}

	mng := workmanager.New(2, forwardToGeneratorFunc)

	executionFinished := make(chan struct{})
	tasks := []model.TaskGenerators{
		//nolint:exhaustruct // it's okay here
		{Generators: []model.Generator{noError{}}},
		//nolint:exhaustruct // it's okay here
		{Generators: []model.Generator{noError{}}},
		//nolint:exhaustruct // it's okay here
		{Generators: []model.Generator{noError{}}},
		//nolint:exhaustruct // it's okay here
		{Generators: []model.Generator{noError{}}},
	}

	var err error
	go func() {
		defer close(executionFinished)

		err = mng.Execute(t.Context(), tasks)
	}()

	<-executionFinished

	require.NoError(t, err)
}
