package utils

import (
	"math"
	"os"
)

var (
	MinInt32 = math.MinInt32
)

func GetRoot(s string) string {
	root := os.Getenv("TEST_ROOT")
	if root == "" {
		root = "/data/muxque"
	}
	return root + s
}
