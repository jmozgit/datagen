package model

import (
	"context"
)

type AcceptanceReason int

const (
	AcceptanceReasonColumnType AcceptanceReason = iota + 1
	AcceptanceReasonDriverAwareness
	AcceptanceReasonReference
	AcceptanceUserSettings
	AcceptanceReasonDomain
	AcceptanceReasonColumnNameSuggestion
)

type Generator interface {
	Gen(ctx context.Context) (any, error)
	Close()
}

type LOGenerator interface {
}

type ChooseCallback func()

type AcceptanceDecision struct {
	Generator  Generator
	AcceptedBy AcceptanceReason
	// Registry should use this callback to notify generator that it has been accepted
	// Might be nil
	ChooseCallback ChooseCallback
}
