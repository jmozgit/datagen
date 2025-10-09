package reference

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/viktorkomarov/datagen/internal/model"
)

type BufferedValues struct {
	reader    model.ValuesReader
	targetID  model.Identifier
	genID     model.Identifier
	batchSize int
	next      chan any
}

func NewBufferedValuesGenerator(
	schema model.DatasetSchema,
	refTargetID model.Identifier,
	refCellID model.Identifier,
	reader model.ValuesReader,
	refresolver model.ReferenceResolver,
	bufferedSize int,
) (model.Generator, model.ChooseCallback) {
	buf := &BufferedValues{
		targetID:  refTargetID,
		genID:     refCellID,
		reader:    reader,
		batchSize: bufferedSize,
		next:      make(chan any, bufferedSize),
	}

	return buf, func() {
		refresolver.Register(schema.ID, refTargetID, buf.onTargetSavedValues)
	}
}

func (b *BufferedValues) Gen(ctx context.Context) (any, error) {
	val, err := b.waitNextValue(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: buffered gen", err)
	}

	return val, nil
}

func (b *BufferedValues) Close() {
	close(b.next)
}

func (b *BufferedValues) waitNextValue(ctx context.Context) (any, error) {
	for {
		select {
		case val := <-b.next:
			return val, nil
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: wait next value", ctx.Err())
		default:
			b.fallbackRead(ctx)
		}
	}
}

func (b *BufferedValues) fallbackRead(ctx context.Context) {
	values, err := b.reader.ReadValues(ctx)
	if err != nil {
		slog.Error("failed to read values", slog.Any("error", err))
		return
	}

	go func() {
		for _, val := range values {
			select {
			case b.next <- val:
			default:
				continue
			}
		}
	}()
}

func (b *BufferedValues) onTargetSavedValues(
	batch model.SaveBatch,
) {
	idx := 0
	for i := 1; i < len(batch.Schema.DataTypes); i++ {
		if batch.Schema.DataTypes[i].SourceName == b.genID {
			idx = i
			break
		}
	}

	for _, row := range batch.Data {
		select {
		case b.next <- row[idx]:
		default:
			continue
		}
	}
}
