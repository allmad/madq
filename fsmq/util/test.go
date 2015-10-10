package util

import (
	"os"
	"testing"

	"gopkg.in/logex.v1"
)

func Defer(t *testing.T, err *error) {
	if *err != nil {
		logex.DownLevel(1).Error(*err)
		t.Fatal(*err)
	}
}

func GetRoot(s string) string {
	root := os.Getenv("TEST_ROOT")
	if root == "" {
		root = "/data/fsmq"
	}
	return root + s
}
