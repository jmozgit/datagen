package integer

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator"
	"github.com/viktorkomarov/datagen/internal/model"

	"github.com/samber/lo"
	"github.com/samber/mo"
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

type Option func(g *Generator)

func WithFormat(format Format) Option {
	return func(g *Generator) {
		g.format = format
	}
}

func WithMinValue(minV int64) Option {
	return func(g *Generator) {
		g.min = minV
	}
}

func WithMaxValue(maxV uint64) Option {
	return func(g *Generator) {
		g.max = maxV
	}
}

type Generator struct {
	format Format
	size   int8
	min    int64
	max    uint64
}

//nolint:gochecknoglobals // more convenient that constants here
var minValidIntegerForSize = map[string]int64{
	"8":   math.MinInt8,
	"8u":  0,
	"16":  math.MinInt16,
	"16u": 0,
	"32":  math.MinInt32,
	"32u": 0,
	"64":  math.MinInt64,
	"64u": 0,
}

//nolint:gochecknoglobals // more convenient that constants here
var maxValidIntergerForSize = map[string]uint64{
	"8":   math.MaxInt8,
	"8u":  math.MaxUint8,
	"16":  math.MaxInt16,
	"16u": math.MaxUint16,
	"32":  math.MaxInt32,
	"32u": math.MaxUint32,
	"64":  math.MaxInt64,
	"64u": math.MaxUint64,
}

func defaultOptions(size int8) (Generator, error) {
	key := fmt.Sprint(size)

	minV, ok := minValidIntegerForSize[key]
	if !ok {
		return Generator{}, fmt.Errorf("%w: %d is unknown for min value", ErrUnsupportedSize, size)
	}

	maxV, ok := maxValidIntergerForSize[key]
	if !ok {
		return Generator{}, fmt.Errorf("%w: %d is unknown for max value", ErrUnsupportedSize, size)
	}

	return Generator{
		format: FormatRandom,
		size:   size,
		min:    minV,
		max:    maxV,
	}, nil
}

func (g Generator) validate() error {
	switch g.format {
	case FormatRandom, FormatSerial:
	default:
		return fmt.Errorf("%w: validate", ErrUnknownFormat)
	}

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

func (g Generator) Gen(_ context.Context) (any, error) {
	return nil, nil //nolint:nilnil // come back later
}

func Accept(
	_ context.Context,
	userSettings config.Generator,
	optBaseType mo.Option[model.TargetType],
) (generator.AcceptanceDecision, error) {
	baseType := optBaseType.OrEmpty()

	if userSettings.Type != config.GeneratorTypeInteger {
		return generator.AcceptanceDecision{}, fmt.Errorf("%w: accept", generator.ErrGeneratorDeclined)
	}

	if baseType.Type != model.Integer {
		return generator.AcceptanceDecision{}, fmt.Errorf("%w: integer generator isn't comparable with %s", generator.ErrSupportOnlyDirectMappings, baseType.SourceType)
	}

	integerCfg := userSettings.Integer

	size := lo.FromPtrOr(integerCfg.BitSize, int8(baseType.FixedSize))
	opts := make([]Option, 0)
	if integerCfg.Format != nil {
		opts = append(opts, WithFormat(Format(*integerCfg.Format)))
	}
	if integerCfg.MaxValue != nil {
		opts = append(opts, WithMaxValue(*integerCfg.MaxValue))
	}
	if integerCfg.MinValue != nil {
		opts = append(opts, WithMinValue(*integerCfg.MinValue))
	}

	gen, err := New(size, opts...)
	if err != nil {
		return generator.AcceptanceDecision{}, fmt.Errorf("%w: accept", err)
	}

	return generator.AcceptanceDecision{
		Generator:  gen,
		AcceptedBy: generator.AcceptanceReasonColumnType,
	}, nil
}

func New(size int8, opts ...Option) (model.Generator, error) {
	genOpts, err := defaultOptions(size)
	if err != nil {
		return Generator{}, fmt.Errorf("%w: new", err)
	}

	for _, opt := range opts {
		opt(&genOpts)
	}

	if err := genOpts.validate(); err != nil {
		return Generator{}, fmt.Errorf("%w: new", err)
	}

	return genOpts, nil
}
