package uuid

import (
	"context"
	"fmt"

	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/pkg/xrand"

	gouuid "github.com/gofrs/uuid"
)

const systemNameLen = 10

type uuidV1Generator struct{}

func NewUUIDV1Generator() model.Generator {
	return uuidV1Generator{}
}

func (u uuidV1Generator) Gen(_ context.Context) (any, error) {
	val, err := gouuid.NewV1()
	if err != nil {
		return nil, fmt.Errorf("%w: uuid v1 gen", err)
	}

	return val, nil
}

func (u uuidV1Generator) Close() {}

func NewUUIDV3Generator() model.Generator {
	return uuidV3Generator{}
}

type uuidV3Generator struct{}

func (u uuidV3Generator) Gen(_ context.Context) (any, error) {
	v4, err := gouuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("%w: uuid v3 gen", err)
	}

	return gouuid.NewV3(v4, xrand.LowerCaseString(systemNameLen)), nil
}

func (u uuidV3Generator) Close() {}

func NewUUIDV4Generator() model.Generator {
	return uuidV4Generator{}
}

type uuidV4Generator struct{}

func (u uuidV4Generator) Gen(_ context.Context) (any, error) {
	val, err := gouuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("%w: uuid v4 gen", err)
	}

	return val, nil
}

func (u uuidV4Generator) Close() {}

func NewUUIDV5Generator() model.Generator {
	return uuidV5Generator{}
}

type uuidV5Generator struct{}

func (u uuidV5Generator) Gen(_ context.Context) (any, error) {
	v4, err := gouuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("%w: uuid v3 gen", err)
	}

	return gouuid.NewV5(v4, xrand.LowerCaseString(systemNameLen)), nil
}

func (u uuidV5Generator) Close() {}

func NewUUIDV6Generator() model.Generator {
	return uuidV6Generator{}
}

type uuidV6Generator struct{}

func (u uuidV6Generator) Gen(_ context.Context) (any, error) {
	val, err := gouuid.NewV6()
	if err != nil {
		return nil, fmt.Errorf("%w: uuid v6 gen", err)
	}

	return val, nil
}

func (u uuidV6Generator) Close() {}

func NewUUIDV7Generator() model.Generator {
	return uuidV7Generator{}
}

type uuidV7Generator struct{}

func (u uuidV7Generator) Gen(_ context.Context) (any, error) {
	val, err := gouuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("%w: uuid v7 gen", err)
	}

	return val, nil
}

func (u uuidV7Generator) Close() {}
