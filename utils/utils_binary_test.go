package utils

import (
	"bytes"
	"testing"
)

func TestByteStr(t *testing.T) {
	hello := "[1 2 23 4 5]"
	expect := []byte{1, 2, 23, 4, 5}
	if !bytes.Equal(MustByteStr(hello), expect) {
		t.Fatal("result not expect")
	}

	hello = "[1 sa 23 5]"
	expect = []byte{1}
	msg, err := ByteStr(hello)
	if !bytes.Equal(expect, msg) || err == nil {
		t.Fatal("result not expect")
	}
}

func TestPathEncode(t *testing.T) {
	if PathEncode("helllo:df") != "helllo_df" {
		t.Fatal("result not expect")
	}
}

func TestRandInt(t *testing.T) {
	if a := RandInt(1); a > 1 || a < 0 {
		t.Fatal("result not expect")
	}
}

func TestRandString(t *testing.T) {
	if a := RandString(3); len(a) < 0 || len(a) > 3 {
		t.Fatal("result not expect")
	}
}
