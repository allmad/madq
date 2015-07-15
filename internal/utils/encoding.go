package utils

import (
	"encoding/binary"
	"io"
	"strings"

	"gopkg.in/logex.v1"
)

var (
	pathReplacer = strings.NewReplacer(":", "_")
)

func PathEncode(p string) string {
	return pathReplacer.Replace(p)
}

func BinaryWriteMulti(w io.Writer, objs []interface{}) (err error) {
	for i := 0; i < len(objs); i++ {
		err = binary.Write(w, binary.LittleEndian, objs[i])
		if err != nil {
			return logex.Trace(err, i)
		}
	}
	return nil
}
