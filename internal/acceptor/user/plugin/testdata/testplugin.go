package main

import "context"

var globalCnt int

func Gen(_ context.Context) (any, error) {
	globalCnt++
	return globalCnt, nil
}
