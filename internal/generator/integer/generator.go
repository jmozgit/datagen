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
	FormatRandom Format = "eandom"
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

func Accept(_ context.Context, userValues any, optBaseType mo.Option[model.TargetType]) (model.Generator, error) {
	baseType := optBaseType.OrEmpty()
	userCfg, ok := userValues.(*config.Integer)
	if !ok || baseType.Type != model.Integer {
		return nil, generator.ErrGeneratorDeclined
	}

	size := lo.FromPtrOr(userCfg.BitSize, int8(baseType.FixedSize))
	opts := make([]Option, 0)
	if userCfg.Format != nil {
		opts = append(opts, WithFormat(Format(*userCfg.Format)))
	}
	if userCfg.MaxValue != nil {
		opts = append(opts, WithMaxValue(*userCfg.MaxValue))
	}
	if userCfg.MinValue != nil {
		opts = append(opts, WithMinValue(*userCfg.MinValue))
	}

	return New(size, opts...)
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
