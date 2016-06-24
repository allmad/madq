package bio

import "fmt"

type Hybrid struct {
	ReadWriterAt
}

func NewHybrid(rw ReadWriterAt) *Hybrid {
	return &Hybrid{rw}
}

func (h *Hybrid) ReadData(off int64, n int) ([]byte, error) {
	buf := make([]byte, n)
	n, err := h.ReadWriterAt.ReadAt(buf, off)
	if err != nil {
		return nil, err
	}
	if len(buf) != n {
		return nil, fmt.Errorf("short read")
	}
	return buf, nil
}
