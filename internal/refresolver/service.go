package refresolver

import (
	"github.com/jmozgit/datagen/internal/model"
)

type Service struct {
	deps map[model.TableName][]model.TableName
	subs map[model.TableName][]model.Subscription
}

func NewService() *Service {
	return &Service{
		deps: make(map[model.TableName][]model.TableName),
		subs: make(map[model.TableName][]model.Subscription),
	}
}

func (s *Service) Register(from, to model.TableName, subFn model.Subscription) {
	s.subs[to] = append(s.subs[to], subFn)
	if from != to {
		s.deps[from] = append(s.deps[from], to)
	}
}

func (s *Service) DepsOn() map[model.TableName][]model.TableName {
	return s.deps
}

func (s *Service) OnProcessed(batch model.SaveBatch) {
	for _, subFn := range s.subs[batch.Schema.TableName] {
		subFn(batch)
	}
}
