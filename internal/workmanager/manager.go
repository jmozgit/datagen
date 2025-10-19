package workmanager

import (
	"context"
	"fmt"
	"sync"

	"github.com/viktorkomarov/datagen/internal/model"
)

type Job func(ctx context.Context, task model.Task) error

type Manager struct {
	workerCnt int
	job       Job
}

func New(workerCnt int, job Job) *Manager {
	return &Manager{
		workerCnt: workerCnt,
		job:       job,
	}
}

func (m *Manager) Execute(
	ctx context.Context,
	tasks []model.Task,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	taskCh := make(chan model.Task)
	closeTaskCh := func() { close(taskCh) }

	errCh := make(chan error)
	defer close(errCh)

	wgWorkers := m.startWorkers(ctx, taskCh, errCh)

	finishWithErr := func(err error) error {
		cancel()
		closeTaskCh()
		_ = waitFinish(wgWorkers, errCh)

		return fmt.Errorf("%w: execute", err)
	}

	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return finishWithErr(ctx.Err())
		case err := <-errCh:
			return finishWithErr(err)
		case taskCh <- task:
		}
	}

	closeTaskCh()

	if err := waitFinish(wgWorkers, errCh); err != nil {
		return fmt.Errorf("%w: execute", err)
	}

	return nil
}

func (m *Manager) startWorkers(
	ctx context.Context,
	taskCh <-chan model.Task,
	errCh chan<- error,
) *sync.WaitGroup {
	var wg sync.WaitGroup

	for range m.workerCnt {
		wg.Add(1)
		go func() {
			defer wg.Done()

			m.work(ctx, taskCh, errCh)
		}()
	}

	return &wg
}

func (m *Manager) work(
	ctx context.Context,
	tasks <-chan model.Task,
	errCh chan<- error,
) {
	const fnName = "work"

	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-tasks:
			if !ok {
				return
			}

			if err := m.job(ctx, task); err != nil {
				errCh <- fmt.Errorf("%w: %s", err, fnName)

				return
			}
		}
	}
}

func waitFinish(
	wg *sync.WaitGroup,
	errCh <-chan error,
) error {
	const fnName = "wait finish"

	finish := make(chan struct{})
	go func() {
		defer close(finish)

		wg.Wait()
	}()

	var err error
	for {
		select {
		case <-finish:
			if err != nil {
				return fmt.Errorf("%w: %s", err, fnName)
			}

			return nil
		case wErr := <-errCh:
			if err == nil {
				err = wErr
			}
		}
	}
}
