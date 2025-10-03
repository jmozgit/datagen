package model

import (
	"context"
)

type AcceptanceReason int

const (
	AcceptanceReasonColumnType AcceptanceReason = iota + 1
	AcceptanceReasonDriverAwareness
	AcceptanceUserSettings
	AcceptanceReasonDomain
	AcceptanceReasonColumnNameSuggestion
)

type Generator interface {
	Gen(ctx context.Context) (any, error)
	Close()
}

type AcceptanceDecision struct {
	Generator  Generator
	AcceptedBy AcceptanceReason
}
