package lua

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/model"

	golua "github.com/yuin/gopher-lua"
)

var ErrUnsupportedLuaReturnedValue = errors.New("unsupported lua returned value")

type scriptExecutor struct {
	state *golua.LState
	path  string
}

func NewScriptExecutor(path string) model.Generator {
	return &scriptExecutor{path: path, state: golua.NewState()}
}

func (s *scriptExecutor) Gen(_ context.Context) (any, error) {
	const fnName = "lua: gen"

	err := s.state.DoFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, fnName)
	}

	val := s.state.Get(-1)
	s.state.Pop(1)

	switch v := val.(type) {
	case golua.LBool:
		return bool(v), nil
	case golua.LString:
		return string(v), nil
	case golua.LNumber:
		return float64(v), nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedLuaReturnedValue, fnName)
	}
}
