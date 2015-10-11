package qrand

import (
	"math/rand"
	"time"
)

var (
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func init() {
	rand.Seed(time.Now().Unix())
}

func RandInt(n int) int {
	return rand.Intn(n)
}

func RandBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

func RandString(n int) string {
	return string(RandBytes(n))
}
