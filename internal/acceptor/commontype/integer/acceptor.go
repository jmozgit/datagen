package integer

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/generator/integer"
	"github.com/viktorkomarov/datagen/internal/model"
)

var ErrUnknownByteSize = errors.New("unknown byte size")

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "commont type integer: accept"

	baseType, ok := req.BaseType.Get()
	if !ok || baseType.Type != model.Integer {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	const (
		int8Gen  = 8
		int32Gen = 32
		int64Gen = 64
	)

	switch baseType.FixedSize {
	case 0, int32Gen:
		return model.AcceptanceDecision{
			AcceptedBy: model.AcceptanceReasonColumnType,
			Generator:  integer.NewRandomInRangeGenerator(math.MinInt32, math.MaxInt32),
		}, nil
	case int8Gen:
		return model.AcceptanceDecision{
			AcceptedBy: model.AcceptanceReasonColumnType,
			Generator:  integer.NewRandomInRangeGenerator(math.MinInt8, math.MaxInt8),
		}, nil
	case int64Gen:
		return model.AcceptanceDecision{
			AcceptedBy: model.AcceptanceReasonColumnType,
			Generator:  integer.NewRandomInRangeGenerator(math.MinInt64, math.MaxInt64),
		}, nil
	default:
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", ErrUnknownByteSize, fnName)
	}
}
