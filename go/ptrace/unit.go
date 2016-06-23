package ptrace

import (
	"fmt"
	"sync/atomic"
)

type Ratio struct {
	Count Int
	Total Int
}

func (r *Ratio) HitIf(n bool) {
	if n {
		r.Hit()
	} else {
		r.Miss()
	}
}

func (r *Ratio) HitN(n int) {
	r.Count.AddInt(n)
	r.Total.Add(1)
}

func (r *Ratio) Hit() {
	r.Count.Add(1)
	r.Total.Add(1)
}

func (r *Ratio) Miss() {
	r.Total.Add(1)
}

func (r *Ratio) MarshalJSON() ([]byte, error) {
	if r.Total == 0 {
		return strJSON("NaN")
	}
	return strJSON(fmt.Sprintf("%.2f%%", float64(r.Count/r.Total*100)))
}

type Int int64

func (i *Int) AddInt(n int) int {
	return int(i.Add(int64(n)))
}

func (i *Int) Add(n int64) int64 {
	return atomic.AddInt64((*int64)(i), n)
}

type Size int64

func (s *Size) AddInt(n int) int {
	return (*Int)(s).AddInt(n)
}

func (s *Size) Add(n int64) int64 {
	return (*Int)(s).Add(n)
}

func (i *Size) MarshalJSON() ([]byte, error) {
	unit := Unit(atomic.LoadInt64((*int64)(i)))
	return strJSON(unit)
}
