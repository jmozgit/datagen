package refresolver

import "github.com/viktorkomarov/datagen/internal/model"

type Service struct {
	subs map[model.Identifier][]model.Subscription
}

func NewService() *Service {
	return &Service{
		subs: make(map[model.Identifier][]model.Subscription),
	}
}

func (s *Service) Register(id model.Identifier, subFn model.Subscription) {
	s.subs[id] = append(s.subs[id], subFn)
}

func (s *Service) OnSaved(batch model.SaveBatch) {
	for _, subFn := range s.subs[batch.Schema.ID] {
		subFn(batch)
	}
}
