package xrand

import "math/rand/v2"

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func LowerCaseString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.IntN(len(letterRunes))]
	}
	return string(b)
}
