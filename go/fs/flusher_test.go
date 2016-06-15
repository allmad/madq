package fs

import (
	"fmt"
	"testing"
	"time"

	"github.com/chzyer/flow"
	"github.com/chzyer/test"
)

var _ FlushDelegate = new(flusherDumpDelegate)

type flusherDumpDelegate struct {
	md *test.MemDisk
}

func (m *flusherDumpDelegate) ReadData(addr ShortAddr, n int) ([]byte, error) {
	buf := make([]byte, n)
	n, err := m.md.ReadAt(buf, int64(addr))
	if err != nil {
		return nil, err
	}
	if n != len(buf) {
		return nil, fmt.Errorf("short read")
	}
	return buf, nil
}

func (m *flusherDumpDelegate) WriteData(addr ShortAddr, b []byte) error {
	n, err := m.md.WriteAt(b, int64(addr))
	if err != nil {
		return err
	}
	if n != len(b) {
		return fmt.Errorf("short written")
	}
	return nil
}

func TestFlusher(t *testing.T) {
	defer test.New(t)

	delegate := &flusherDumpDelegate{test.NewMemDisk(0)}
	f := flow.New()
	flusher := NewFlusher(f, time.Second, delegate)
	flusher.Close()
}
