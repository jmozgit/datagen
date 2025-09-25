package closer

import (
	"context"
	"errors"
)

type Registry struct {
	closers []Closer
}

func NewRegistry() *Registry {
	return &Registry{
		closers: make([]Closer, 0),
	}
}

func (c *Registry) Add(closer Closer) {
	c.closers = append(c.closers, closer)
}

func (c *Registry) Close(ctx context.Context) error {
	errs := make([]error, 0)
	for _, closer := range c.closers {
		err := closer.Close(ctx)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

type Closer interface {
	Close(ctx context.Context) error
}

type Fn func()

func (c Fn) Close(_ context.Context) error {
	c()

	return nil
}
