package inet

import (
	"context"
	"math/rand/v2"
	"reflect"

	"github.com/go-faker/faker/v4"
	"github.com/viktorkomarov/datagen/internal/model"
)

type generator struct {
	internet faker.Networker
}

func NewPostgresql() model.Generator {
	return generator{
		internet: faker.GetNetworker(),
	}
}

func (g generator) Gen(_ context.Context) (any, error) {
	if rand.Int()%2 == 0 {
		return g.internet.IPv4(reflect.Value{})
	}

	return g.internet.IPv6(reflect.Value{})
}

func (g generator) Close() {}
