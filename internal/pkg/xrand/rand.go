package xrand

import "math/rand/v2"

//nolint:gochecknoglobals // more convenient that constants here
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func LowerCaseString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.IntN(len(letterRunes))] //nolint:gosec // no problem here
	}

	return string(b)
}

var letters = append(letterRunes, []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_!@#$%^&*()><:")...)

func String(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))] //nolint:gosec // no problem here
	}

	return string(b)
}
