package model

import (
	"context"

	"github.com/samber/mo"
	"github.com/viktorkomarov/datagen/internal/config"
)

type AcceptanceReason int

const (
	AcceptanceReasonColumnType AcceptanceReason = iota + 1
	AcceptanceReasonDomain
	AcceptanceReasonColumnNameSuggestion
)

type GeneratorProvider interface {
	Accept(
		ctx context.Context,
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
