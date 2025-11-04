package oid

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	mathrand "math/rand/v2"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmozgit/datagen/internal/model"
)

type ApproximatelySizedGenerator struct {
	pool           *pgxpool.Pool
	sizedBytes     int64
	changeRangeAbs int64
	resolver       model.ReferenceResolver
	dispatcher     *sizeDispactcher
	notify         chan []model.LOGenerated
}

func NewApproximatelySizedGenerator(
	pool *pgxpool.Pool,
	sizedBytes int64, changeRangeAbs int64,
	resolver model.ReferenceResolver,
	tableName model.TableName,
	column model.Identifier,
) (model.LOGenerator, func()) {
	g := &ApproximatelySizedGenerator{
		pool:           pool,
		sizedBytes:     sizedBytes,
		changeRangeAbs: changeRangeAbs,
		resolver:       resolver,
		notify:         make(chan []model.LOGenerated),
	}
	return g, func() {
		g.dispatcher = newSizeDispatcher(pool, column, g.notify)
		resolver.Register(tableName, tableName, g.dispatcher.OnSaved())
	}
}

func (g *ApproximatelySizedGenerator) Gen(ctx context.Context) (any, error) {
	sign := mathrand.Int() % 2
	if sign == 0 {
		sign = -1
	}
	rng := g.changeRangeAbs
	if g.changeRangeAbs > 0 {
		rng = mathrand.Int64N(g.changeRangeAbs)
	}

	oid, err := g.createAndFillOID(ctx, g.sizedBytes+int64(sign)*rng)
	if err != nil {
		return nil, fmt.Errorf("%w: gen", err)
	}

	return oid, nil
}

func (g *ApproximatelySizedGenerator) Close() {}

func (g *ApproximatelySizedGenerator) LOGeneratedChan() <-chan []model.LOGenerated {
	return g.notify
}

func (g *ApproximatelySizedGenerator) createAndFillOID(ctx context.Context, size int64) (oid uint32, outErr error) {
	tx, err := g.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("%w: create and fill oid", err)
	}
	defer func() {
		if outErr == nil {
			outErr = tx.Commit(ctx)
		} else {
			_ = tx.Rollback(ctx)
		}
	}()

	lo := tx.LargeObjects()

	oid, err = lo.Create(ctx, 0)
	if err != nil {
		return 0, fmt.Errorf("%w: create and fill oid", err)
	}

	obj, err := lo.Open(ctx, oid, pgx.LargeObjectModeWrite)
	if err != nil {
		return 0, fmt.Errorf("%w: create and fill oid", err)
	}
	defer obj.Close()

	if _, err := io.CopyN(obj, rand.Reader, size); err != nil {
		return 0, fmt.Errorf("%w: create and fill oid", err)
	}

	g.dispatcher.oidGenerated(oid, uint64(size))

	return oid, nil
}
