package chans

import (
	"sync"
)

func Discards[T any](values ...<-chan T) {
	for _, val := range values {
		go func(val <-chan T) {
			for range val {
			}
		}(val)
	}
}

func FanIn[T any](values ...<-chan T) <-chan T {
	out := make(chan T)

	var wg sync.WaitGroup

	wg.Add(len(values))
	for _, val := range values {
		go func(val <-chan T) {
			defer wg.Done()

			for v := range val {
				out <- v
			}
		}(val)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
