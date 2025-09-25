package generator

import (
	"errors"
)

var (
	ErrGeneratorDeclined     = errors.New("generator is declined")
	ErrNoAvailableGenerators = errors.New("no available generators")
)
