package oid

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmozgit/datagen/internal/model"
)

type sizeDispactcher struct {
	pool          *pgxpool.Pool
	column        model.Identifier
	notify        chan []model.LOGenerated
	mu            sync.Mutex
	generatedOIDs map[uint32]uint64
}

func newSizeDispatcher(
	pool *pgxpool.Pool,
	column model.Identifier,
	notify chan []model.LOGenerated,
) *sizeDispactcher {
	return &sizeDispactcher{
		pool:          pool,
		notify:        notify,
		column:        column,
		generatedOIDs: make(map[uint32]uint64),
	}
}

func (s *sizeDispactcher) columnIdx(b model.SaveBatch) int {
	for idx, col := range b.Schema.Columns {
		if col.SourceName == s.column {
			return idx
		}
	}

	return -1
}

func (s *sizeDispactcher) Close() {
	close(s.notify)
}

func (s *sizeDispactcher) OnSaved() model.Subscription {
	return func(batch model.SaveBatch) {
		colIdx := s.columnIdx(batch)
		if colIdx == -1 {
			return
		}

		saved := make([]uint32, 0)

		for i := range batch.Data {
			oid, ok := batch.Data[i][colIdx].(uint32)
			if !ok {
				slog.Debug("mismtach oid type")
				continue
			}

			if batch.Invalid[i] {
				if err := s.oidInRowDiscared(context.Background(), oid); err != nil {
					slog.Error("failed to unlink oid", slog.Any("error", err))
					continue
				}
			} else {
				saved = append(saved, oid)
			}
		}

		s.oidInRowInserted(context.Background(), saved)
	}
}

func (s *sizeDispactcher) oidGenerated(oid uint32, size uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.generatedOIDs[oid] = size
}

func (s *sizeDispactcher) oidInRowDiscared(ctx context.Context, oid uint32) error {
	s.mu.Lock()
	_, ok := s.generatedOIDs[oid]
	if ok {
		delete(s.generatedOIDs, oid)
	}
	s.mu.Unlock()

	if !ok {
		return nil
	}

	_, err := s.pool.Exec(ctx, "SELECT lo_unlink($1)", oid)
	if err != nil {
		return fmt.Errorf("%w: oid in row discared", err)
	}

	return nil
}

func (s *sizeDispactcher) oidInRowInserted(ctx context.Context, oids []uint32) error {
	msg := make([]model.LOGenerated, 0)
	s.mu.Lock()
	for _, oid := range oids {
		size, ok := s.generatedOIDs[oid]
		if ok {
			delete(s.generatedOIDs, oid)
			msg = append(msg, model.LOGenerated{Size: size})
		}
	}
	s.mu.Unlock()

	if len(msg) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.notify <- msg:
		}
	}

	return nil
}
