package reference

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/viktorkomarov/datagen/internal/model"
)

var Sink any

type BufferedValues struct {
	columnReader model.ColumnValueReader
	refTable     model.TableName
	refCol       model.Identifier
	batchSize    int
	next         chan any
}

func NewBufferedValuesGenerator(
	schema model.DatasetSchema,
	columnReader model.ColumnValueReader,
	refTable model.TableName,
	refCol model.Identifier,
	refresolver model.ReferenceResolver,
	bufferedSize int,
) (model.Generator, model.ChooseCallback) {
	buf := &BufferedValues{
		columnReader: columnReader,
		refTable:     refTable,
		refCol:       refCol,
		batchSize:    bufferedSize,
		next:         make(chan any, bufferedSize),
	}

	return buf, func() {
		refresolver.Register(schema.TableName, refTable, buf.onTargetSavedValues)
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
	values, err := b.columnReader.ReadValues(ctx)
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
	for i := 1; i < len(batch.Schema.Columns); i++ {
		if batch.Schema.Columns[i].SourceName == b.refCol {
			idx = i
			break
		}
	}

	for i, row := range batch.Data {
		if !batch.IsValid(i) {
			continue
		}

		select {
		case b.next <- row[idx]:
		default:
			continue
		}
	}
}
