package qio

import (
	"os"
	"testing"

	"github.com/chzyer/fsmq/fsmq/util/test"
)

var root = test.Root("/test/util/qio")

func TestReaderWriter(t *testing.T) {
	defer test.New(t)

	var n int
	f, err := os.Create(root)
	test.Nil(err)

	w := NewWriter(f, 1)
	offset, _ := w.Seek(-1, 1)
	test.NotEqual(offset, 0)

	_, err = w.Write([]byte("caoo"))
	test.Nil(err)
	defer w.Close()
	// |caoo

	_, err = w.Seek(0, 2)
	test.Equal(err, ErrSeekNotSupport)

	_, err = w.Seek(1, 0)
	test.Nil(err)

	n, err = w.Write([]byte("j"))
	test.Equals(err, nil, n, 1)
	// cj|oo

	r := NewReader(f, 0)
	defer r.Close()

	_, err = r.Seek(1, 1)
	test.Nil(err)
	// c|joo

	_, err = r.Seek(0, 2)
	test.Equal(err, ErrSeekNotSupport)

	buf := make([]byte, 3)
	n, err = r.Read(buf)
	test.Equals(n, 3, err, nil, buf, []byte("joo"))
	// cjoo|

	_, err = r.Seek(0, 0)
	test.Nil(err)
}
