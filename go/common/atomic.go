package common

import (
	"fmt"
	"sync/atomic"
)

type Int int64

func (i *Int) AddInt(n int) int {
	return int(i.Add(int64(n)))
}

func (i *Int) Add(n int64) int64 {
	return atomic.AddInt64((*int64)(i), n)
}

func (i *Int) MarshalJSON() ([]byte, error) {
	unit := Unit(atomic.LoadInt64((*int64)(i)))
	return []byte(`"` + unit + `"`), nil
}

func Unit(a int64) string {
	units := []string{"b", "kb", "mb", "gb", "pb"}
	n := float64(a)
	unitIdx := 0
	for n > 1024 {
		n /= 1024
		unitIdx++
	}
	return fmt.Sprintf("%.2f%v", n, units[unitIdx])
}
