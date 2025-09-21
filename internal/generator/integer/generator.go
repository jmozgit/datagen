package integer

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator"
	"github.com/viktorkomarov/datagen/internal/model"

	"github.com/samber/mo"
)

var (
	ErrUnknownFormat        = errors.New("format is unknown")
	ErrUnsupportedSize      = errors.New("size in unsupported")
	ErrIncorrectMinMaxValue = errors.New("min max value are incorrect")
)

type Format string

const (
	FormatUnspecified Format = ""
	FormatRandom      Format = "random"
	FormatSerial      Format = "serial"
)

type Option func(g *options)

func WithFormat(format Format) Option {
	return func(g *options) {
		g.format = format
	}
}

func WithMinValue(minV int64) Option {
	return func(g *options) {
		g.min = minV
	}
}

func WithMaxValue(maxV int64) Option {
	return func(g *options) {
		g.max = maxV
	}
}

type options struct {
	format Format
	size   int8
	min    int64
	max    int64
}

func Accept(
	_ context.Context,
	optUserSettings mo.Option[config.Generator],
	optBaseType mo.Option[model.TargetType],
) (generator.AcceptanceDecision, error) {
	userSettings, userPresented := optUserSettings.Get()
	if userPresented && userSettings.Type != config.GeneratorTypeInteger {
		return generator.AcceptanceDecision{}, fmt.Errorf("%w: accept", generator.ErrGeneratorDeclined)
	}
	baseType, baseTypePresented := optBaseType.Get()
	if !userPresented && baseTypePresented && baseType.Type != model.Integer {
		return generator.AcceptanceDecision{}, fmt.Errorf("%w: accept", generator.ErrGeneratorDeclined)
	}
	size := int8(baseType.FixedSize)
	if userPresented && userSettings.Integer.ByteSize != nil {
		size = *userSettings.Integer.ByteSize
	}

	opts := make([]Option, 0)
	if userPresented && userSettings.Integer.Format != nil {
		opts = append(opts, WithFormat(Format(*userSettings.Integer.Format)))
	}
	if userPresented && userSettings.Integer.MaxValue != nil {
		opts = append(opts, WithMaxValue(*userSettings.Integer.MaxValue))
	}
	if userPresented && userSettings.Integer.MinValue != nil {
		opts = append(opts, WithMinValue(*userSettings.Integer.MinValue))
	}

	gen, err := newGenerator(size, optBaseType, opts...)
	if err != nil {
		return generator.AcceptanceDecision{}, fmt.Errorf("%w: accept", err)
	}

	return generator.AcceptanceDecision{
		Generator:  gen,
		AcceptedBy: generator.AcceptanceReasonColumnType,
	}, nil
}

func newGenerator(
	size int8,
	optBaseType mo.Option[model.TargetType],
	opts ...Option,
) (model.Generator, error) {
	genOpts, err := defaultOptions(size)
	if err != nil {
		return nil, fmt.Errorf("%w: new", err)
	}

	for _, opt := range opts {
		opt(&genOpts)
	}

	if err := genOpts.validate(); err != nil {
		return nil, fmt.Errorf("%w: new", err)
	}

	switch genOpts.format {
	case FormatUnspecified:
		baseType, ok := optBaseType.Get()
		if ok && baseType.IsSerial {
			if baseType.SourceSpecifiedDefault != "" {
				return newSourceSpecifiedGenerator(baseType.SourceSpecifiedDefault), nil
			}

			return newSerialGenerator(1), nil
		}

		return newRandomGenerator(genOpts.min, genOpts.max), nil
	case FormatRandom:
		return newRandomGenerator(genOpts.min, genOpts.max), nil
	case FormatSerial:
		return newSerialGenerator(genOpts.min), nil
	default:
		return nil, fmt.Errorf("%w: %s new generator", ErrUnknownFormat, genOpts.format)
	}
}

//nolint:gochecknoglobals // more convenient that constants here
var minValidIntegerForSize = map[string]int64{
	"1":  math.MinInt8,
	"1u": 0,
	"2":  math.MinInt16,
	"2u": 0,
	"4":  math.MinInt32,
	"4u": 0,
	"8":  math.MinInt64,
	"8u": 0,
}

//nolint:gochecknoglobals // more convenient that constants here
var maxValidIntergerForSize = map[string]int64{
	"1":  math.MaxInt8,
	"1u": math.MaxUint8,
	"2":  math.MaxInt16,
	"2u": math.MaxUint16,
	"4":  math.MaxInt32,
	"4u": math.MaxUint32,
	"8":  math.MaxInt64,
	"8u": math.MaxInt64,
}

func defaultOptions(size int8) (options, error) {
	key := fmt.Sprint(size)

	minV, ok := minValidIntegerForSize[key]
	if !ok {
		return options{}, fmt.Errorf("%w: %d is unknown for min value", ErrUnsupportedSize, size)
	}

	maxV, ok := maxValidIntergerForSize[key]
	if !ok {
		return options{}, fmt.Errorf("%w: %d is unknown for max value", ErrUnsupportedSize, size)
	}

	return options{
		format: FormatUnspecified,
		size:   size,
		min:    minV,
		max:    maxV,
	}, nil
}

func (g options) validate() error {
	key := fmt.Sprint(g.size)
	if g.min >= 0 {
		key += "u"
	}

	minV, maxV := minValidIntegerForSize[key], maxValidIntergerForSize[key]

	if g.min >= minV && g.max <= maxV {
		return nil
	}

	return fmt.Errorf(
		"%w: valid range for size %d is [%d, %d], not [%d %d]",
		ErrIncorrectMinMaxValue,
		g.size, minV, maxV, g.min, g.max,
	)
}
