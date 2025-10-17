package network

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"

	"github.com/go-faker/faker/v4"
	"github.com/viktorkomarov/datagen/internal/model"
)

var ErrUnknownNetworkType = errors.New("unknown network type")

type generator func() (any, error)

func NewGenerator(name string) (model.Generator, error) {
	switch name {
	case "inet", "cidr":
		genNet := faker.GetNetworker()
		return generator(func() (any, error) {
			if rand.Int()%2 == 0 {
				return genNet.IPv4(reflect.Value{})
			}

			return genNet.IPv6(reflect.Value{})
		}), nil
	case "macaddr", "macaddr8":
		genNet := faker.GetNetworker()
		return generator(func() (any, error) {
			return genNet.MacAddress(reflect.Value{})
		}), nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownNetworkType, name)
	}
}

func (g generator) Gen(_ context.Context) (any, error) {
	return g()
}

func (g generator) Close() {}
