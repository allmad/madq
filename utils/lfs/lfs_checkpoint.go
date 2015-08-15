package lfs

import (
	"bytes"
	"encoding/gob"
	"io"
	"sync"

	"gopkg.in/logex.v1"
)

type checkPoint struct {
	blkOff int64
	Data   map[string]int64
	sync.Mutex
}

func newCheckPoint() *checkPoint {
	return &checkPoint{
		Data: make(map[string]int64),
	}
}

func (c *checkPoint) PRead() {

}

func (c *checkPoint) Save(w io.Writer) error {
	buf := bytes.NewBuffer(make([]byte, 0, 512))
	if err := gob.NewEncoder(buf).Encode(c.Data); err != nil {
		panic(err)
	}
	_, err := buf.WriteTo(w)
	return logex.Trace(err)
}

func (c *checkPoint) SetInoOffset(name string, offset int64) {
	c.Lock()
	c.Data[name] = offset
	c.Unlock()
}
