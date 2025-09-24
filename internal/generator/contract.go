package generator

import (
	"errors"
)

var (
	ErrGeneratorDeclined              = errors.New("generator is declined")
	ErrNoAvailableGenerators          = errors.New("no available generators")
	ErrSupportOnlyDirectMappings      = errors.New("support only direct mappings")
	ErrAlwaysUseSourceProviderDefault = errors.New("always use source provider default")
)
