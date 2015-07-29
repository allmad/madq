package utils

import (
	"strconv"
	"strings"

	"gopkg.in/logex.v1"
)

func ByteStr(s string) []byte {
	msgStr := strings.Trim(s, "[]\n\r")
	msgByteStr := strings.Split(msgStr, " ")
	msgBytes := make([]byte, len(msgByteStr))
	for i := range msgBytes {
		b, err := strconv.Atoi(msgByteStr[i])
		if err != nil {
			logex.Fatal(err)
		}
		msgBytes[i] = byte(b)
	}
	return msgBytes
}
