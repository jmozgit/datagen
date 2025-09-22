package generator

import (
	"errors"

	"github.com/viktorkomarov/datagen/internal/model"
)

var (
	ErrGeneratorDeclined              = errors.New("generator is declined")
	ErrNoAvailableGenerators          = errors.New("no available generators")
	ErrSupportOnlyDirectMappings      = errors.New("support only direct mappings")
	ErrAlwaysUseSourceProviderDefault = errors.New("always use source provider default")
)

type AcceptanceReason int

const (
	AcceptanceReasonColumnType AcceptanceReason = iota + 1
	AcceptanceReasonDomain
	AcceptanceReasonColumnNameSuggestion
)

type AcceptanceDecision struct {
	Generator  model.Generator
	AcceptedBy AcceptanceReason
}
