package closer

import "context"

type CloserRegistry struct {
	closers []Closer
}

func (c *CloserRegistry) Add(closer Closer) {
	c.closers = append(c.closers, closer)
}

type Closer interface {
	Close(ctx context.Context) error
}

type CloserFn func()

func (c CloserFn) Close(_ context.Context) error {
	c()

	return nil
}
