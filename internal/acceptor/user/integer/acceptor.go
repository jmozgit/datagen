package integer

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator/integer"
	"github.com/viktorkomarov/datagen/internal/model"
)

var (
	ErrUnknownFormat        = errors.New("format is unknown")
	ErrUnsupportedSize      = errors.New("size in unsupported")
	ErrIncorrectMinMaxValue = errors.New("min max value are incorrect")
)

type Format string

const (
	FormatRandom Format = "random"
	FormatSerial Format = "serial"
)

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "user integer: accept"

	userSettings, ok := req.UserSettings.Get()
	if !ok || userSettings.Type != config.GeneratorTypeInteger {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}
	integerSettings := userSettings.Integer

	options, err := buildOptions(integerSettings, req.BaseType)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	switch options.format {
	case FormatRandom:
		if options.byteSize == nil {
			return model.AcceptanceDecision{
				AcceptedBy:     model.AcceptanceUserSettings,
				Generator:      integer.NewRandomInRangeGenerator(math.MinInt32, math.MaxInt32),
				ChooseCallback: nil,
			}, nil
		}
		key := fmt.Sprint(*options.byteSize)
		if *options.minValue >= 0 {
			key += "u"
		}

		minV, maxV := minValidIntegerForSize[key], maxValidIntegerForSize[key]
		if *options.minValue >= minV && *options.maxValue <= maxV {
			return model.AcceptanceDecision{
				AcceptedBy: model.AcceptanceUserSettings,
				Generator: integer.NewRandomInRangeGenerator(
					*options.minValue, *options.maxValue,
				),
				ChooseCallback: nil,
			}, nil
		}

		return model.AcceptanceDecision{}, fmt.Errorf(
			"%w: valid range for size %d is [%d, %d], not [%d %d]",
			ErrIncorrectMinMaxValue,
			*options.byteSize, minV, maxV, *options.minValue, *options.maxValue,
		)
	case FormatSerial:
		if options.minValue == nil {
			return model.AcceptanceDecision{
				AcceptedBy:     model.AcceptanceUserSettings,
				Generator:      integer.NewSerialIntegerGenerator(0),
				ChooseCallback: nil,
			}, nil
		}

		return model.AcceptanceDecision{
			AcceptedBy:     model.AcceptanceUserSettings,
			Generator:      integer.NewSerialIntegerGenerator(*options.minValue),
			ChooseCallback: nil,
		}, nil
	default:
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s %s", ErrUnknownFormat, options.format, fnName)
	}
}
