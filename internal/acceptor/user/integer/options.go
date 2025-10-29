package integer

import (
	"fmt"
	"math"

	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/samber/mo"
)

type options struct {
	format   Format
	byteSize *int8
	minValue *int64
	maxValue *int64
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
var maxValidIntegerForSize = map[string]int64{
	"1":  math.MaxInt8,
	"1u": math.MaxUint8,
	"2":  math.MaxInt16,
	"2u": math.MaxUint16,
	"4":  math.MaxInt32,
	"4u": math.MaxUint32,
	"8":  math.MaxInt64,
	"8u": math.MaxInt64,
}

func buildOptions(
	userSettings *config.Integer,
	baseType mo.Option[model.TargetType],
) (options, error) {
	var opt options

	opt.format = FormatRandom
	if userSettings != nil && userSettings.Format != nil {
		opt.format = Format(*userSettings.Format)
	}

	if userSettings != nil && userSettings.ByteSize != nil {
		opt.byteSize = userSettings.ByteSize
	}
	if opt.byteSize == nil && baseType.IsPresent() {
		tmp := int8(baseType.MustGet().FixedSize)
		opt.byteSize = &tmp
	}

	if userSettings != nil && userSettings.MinValue != nil {
		opt.minValue = userSettings.MinValue
	}

	if opt.format == FormatSerial {
		return opt, nil
	}

	if opt.minValue == nil && opt.byteSize != nil {
		key := fmt.Sprint(*opt.byteSize)
		minV, ok := minValidIntegerForSize[key]
		if !ok {
			return options{}, fmt.Errorf("%w: %s is unknown for min value", ErrUnsupportedSize, key)
		}
		opt.minValue = &minV
	}

	if userSettings != nil && userSettings.MaxValue != nil {
		opt.maxValue = userSettings.MaxValue
	}
	if opt.maxValue == nil && opt.byteSize != nil {
		key := fmt.Sprint(*opt.byteSize)
		maxV, ok := maxValidIntegerForSize[key]
		if !ok {
			return options{}, fmt.Errorf("%w: %s is unknown for max value", ErrUnsupportedSize, key)
		}
		opt.maxValue = &maxV
	}

	return opt, nil
}
