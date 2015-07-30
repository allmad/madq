package utils

import "os"

func GetRoot(s string) string {
	root := os.Getenv("TEST_ROOT")
	if root == "" {
		root = "/data/muxque"
	}
	return root + s
}
