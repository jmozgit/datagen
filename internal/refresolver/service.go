package refresolver

import (
	"github.com/viktorkomarov/datagen/internal/model"
)

type Service struct {
	deps map[model.Identifier][]model.Identifier
	subs map[model.Identifier][]model.Subscription
}

func NewService() *Service {
	return &Service{
		deps: make(map[model.Identifier][]model.Identifier),
		subs: make(map[model.Identifier][]model.Subscription),
	}
}

func (s *Service) Register(from, to model.Identifier, subFn model.Subscription) {
	s.subs[to] = append(s.subs[to], subFn)
	s.deps[from] = append(s.deps[from], to)
}

func (s *Service) DepsOn() map[model.Identifier][]model.Identifier {
	return s.deps
}

func (s *Service) OnSaved(batch model.SaveBatch) {
	for _, subFn := range s.subs[batch.Schema.ID] {
		subFn(batch)
	}
}
