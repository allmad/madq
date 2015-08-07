package cc

import (
	"bytes"
	"testing"
)

func TestByteStr(t *testing.T) {
	hello := "[1 2 23 4 5]"
	expect := []byte{1, 2, 23, 4, 5}
	if !bytes.Equal(ByteStr(hello), expect) {
		t.Fatal("result not expect")
	}
}
