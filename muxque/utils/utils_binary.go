package utils

import (
	"math/rand"
	"strconv"
	"strings"
	"time"
)

func MustByteStr(s string) []byte {
	msg, _ := ByteStr(s)
	return msg
}

func ByteStr(s string) ([]byte, error) {
	msgStr := strings.Trim(s, "[]\n\r")
	msgByteStr := strings.Split(msgStr, " ")
	msgBytes := make([]byte, len(msgByteStr))
	for i := range msgBytes {
		b, err := strconv.Atoi(msgByteStr[i])
		if err != nil {
			return msgBytes[:i], err
		}
		msgBytes[i] = byte(b)
	}
	return msgBytes, nil
}

var (
	pathReplacer = strings.NewReplacer(":", "_")
	letters      = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	randSource   = rand.New(rand.NewSource(time.Now().Unix()))
)

func RandInt(n int) int {
	return randSource.Intn(n)
}

func RandString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randSource.Intn(len(letters))]
	}
	return string(b)
}

func PathEncode(p string) string {
	return pathReplacer.Replace(p)
}
