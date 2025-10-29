package integer

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/generator/integer"
	"github.com/jmozgit/datagen/internal/model"
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
	const fnName = "common type integer: accept"

	baseType, ok := req.BaseType.Get()
	if !ok || baseType.Type != model.Integer {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	const (
		int8Gen  = 1
		int16Gen = 2
		int32Gen = 4
		int64Gen = 8
	)

	switch baseType.FixedSize {
	case 0, int32Gen:
		return model.AcceptanceDecision{
			AcceptedBy:     model.AcceptanceReasonColumnType,
			Generator:      integer.NewRandomInRangeGenerator(math.MinInt32, math.MaxInt32),
			ChooseCallback: nil,
		}, nil
	case int8Gen:
		return model.AcceptanceDecision{
			AcceptedBy:     model.AcceptanceReasonColumnType,
			Generator:      integer.NewRandomInRangeGenerator(math.MinInt8, math.MaxInt8),
			ChooseCallback: nil,
		}, nil
	case int16Gen:
		return model.AcceptanceDecision{
			AcceptedBy:     model.AcceptanceReasonColumnType,
			Generator:      integer.NewRandomInRangeGenerator(math.MinInt16, math.MaxInt16),
			ChooseCallback: nil,
		}, nil
	case int64Gen:
		return model.AcceptanceDecision{
			AcceptedBy:     model.AcceptanceReasonColumnType,
			Generator:      integer.NewRandomInRangeGenerator(math.MinInt64, math.MaxInt64),
			ChooseCallback: nil,
		}, nil
	default:
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", ErrUnknownByteSize, fnName)
	}
}
