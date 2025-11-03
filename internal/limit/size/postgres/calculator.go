package postgres

import "sync"

type calculator struct {
	mu          sync.Mutex
	init        uint64
	staticSize  uint64
	dynamicSize uint64
}

func newCalculator(init uint64) *calculator {
	return &calculator{
		init:        init,
		staticSize:  0,
		dynamicSize: 0,
	}
}

func (c *calculator) resetStaticSize(sz uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.staticSize = sz
}

func (c *calculator) addDynamicSize(sz uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dynamicSize += sz
}

func (c *calculator) collected() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	generated := c.dynamicSize + c.staticSize
	if generated == 0 {
		return 0
	}

	return (generated) - c.init
}
