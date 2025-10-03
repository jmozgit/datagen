package reference

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/model"
)

type Pipe struct {
	Pub model.Generator
	Sub model.Generator
}

func BuildPipe(from model.Generator, bufSize int) Pipe {
	pub := newPub(from)
	sub := newSub(bufSize, pub)

	return Pipe{
		Pub: pub,
		Sub: sub,
	}
}

func newPub(gen model.Generator) pub {
	return pub{
		msg: make(chan any),
		gen: gen,
	}
}

type pub struct {
	msg chan any
	gen model.Generator
}

func (n pub) Gen(ctx context.Context) (any, error) {
	val, err := n.gen.Gen(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: notify wrapper gen", err)
	}

	n.msg <- val

	return val, nil
}

func (n pub) Close() {
	close(n.msg)
}

type sub struct {
	next chan any
}

func newSub(limit int, pub pub) model.Generator {
	b := &sub{
		next: make(chan any, limit),
	}

	go func() {
		for val := range pub.msg {
			select {
			case b.next <- val:
			default:
				continue
			}
		}
	}()

	return b
}

func (s *sub) Gen(ctx context.Context) (any, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: reference gen", ctx.Err())
	case val := <-s.next:
		return val, nil
	}
}

func (s *sub) Close() {}
