package integer

import "context"

type sourceSpecifiedGenerator struct {
	v string
}

func newSourceSpecifiedGenerator(v string) sourceSpecifiedGenerator {
	return sourceSpecifiedGenerator{v: v}
}

func (s sourceSpecifiedGenerator) Gen(_ context.Context) (any, error) {
	return s.v, nil
}
