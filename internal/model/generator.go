package model

import (
	"context"

	"github.com/viktorkomarov/datagen/internal/config"

	"github.com/samber/mo"
)

type AcceptanceReason int

const (
	AcceptanceReasonColumnType AcceptanceReason = iota + 1
	AcceptanceUserSettings
	AcceptanceReasonDomain
	AcceptanceReasonColumnNameSuggestion
)

type GeneratorProvider interface {
	Accept(
		ctx context.Context,
		dataset DatasetSchema,
		userValues mo.Option[config.Generator],
		optBaseType mo.Option[TargetType],
	) (AcceptanceDecision, error)
}

type Generator interface {
	Gen(ctx context.Context) (any, error)
}

type AcceptanceDecision struct {
	Generator  Generator
	AcceptedBy AcceptanceReason
}
