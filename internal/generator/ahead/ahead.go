package ahead

import (
	"context"
	"fmt"
	"sync"

	"github.com/jmozgit/datagen/internal/model"
)

type Generator struct {
	innerGen          model.Generator
	next              chan any
	errCh             chan error
	cancelPreparation chan struct{}

	wg sync.WaitGroup
}

func NewGenerator(g model.Generator) model.Generator {
	gen := &Generator{
		innerGen:          g,
		next:              make(chan any, 1),
		errCh:             make(chan error, 1),
		cancelPreparation: make(chan struct{}, 1),
	}
	gen.prepareNext()

	return gen
}

func (g *Generator) cancelPreparationNext() {
	g.cancelPreparation <- struct{}{}
	g.wg.Wait()
}

func (g *Generator) prepareNext() {
	g.wg.Add(1)

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		messageSend := make(chan struct{})
		defer close(messageSend)

		go func() {
			defer g.wg.Done()

			val, err := g.innerGen.Gen(ctx)
			if ctx.Err() != nil {
				return
			}

			if err != nil {
				g.errCh <- err
				messageSend <- struct{}{}
			} else {
				g.next <- val
				messageSend <- struct{}{}
			}
		}()

		// There is a bug
		select {
		case <-messageSend:
			return
		case <-g.cancelPreparation:
			cancel()
		}
	}()
}

func (g *Generator) Gen(ctx context.Context) (any, error) {
	select {
	case val := <-g.next:
		g.prepareNext()
		return val, nil
	case err := <-g.errCh:
		return nil, fmt.Errorf("%w: ahead gen", err)
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: ahead gen", ctx.Err())
	}
}

func (g *Generator) Close() {
	g.cancelPreparationNext()
	close(g.errCh)
	close(g.next)
	close(g.cancelPreparation)
	g.innerGen.Close()
}
