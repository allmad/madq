package ptrace

import (
	"fmt"
	"sync/atomic"
	"time"
)

type RatioTime struct {
	Duration time.Duration
	Count    Int
}

func (r *RatioTime) AddNow(t time.Time) {
	r.Duration += time.Now().Sub(t)
	r.Count.Add(1)
}

func (r *RatioTime) String() string {
	if r.Count == 0 {
		return "NaN"
	}

	return fmt.Sprintf("%v (%v/%v)",
		r.Duration/time.Duration(r.Count),
		r.Duration,
		r.Count,
	)
}

func (r *RatioTime) MarshalJSON() ([]byte, error) { return strJSON(r.String()) }

// -----------------------------------------------------------------------------

type RatioSize struct {
	Size  Size
	Count Int
}

func (r *RatioSize) AddBuf(b []byte) {
	r.Add(int64(len(b)))
}

func (r *RatioSize) Add(s int64) {
	r.Size.Add(s)
	r.Count.Add(1)
}

func (r RatioSize) String() string {
	if r.Count == 0 {
		return "NaN"
	}
	ratio := float64(r.Size) / float64(r.Count)
	return fmt.Sprintf("%v (%v/%v)", Unit(int64(ratio)), r.Size, r.Count)
}

func (r *RatioSize) MarshalJSON() ([]byte, error) {
	return strJSON(r.String())
}

// -----------------------------------------------------------------------------

type Ratio struct {
	Value Int
	Count Int
}

func (r *Ratio) HitIf(n bool) {
	if n {
		r.Hit()
	} else {
		r.Miss()
	}
}

func (r *Ratio) HitN(n int) {
	r.Value.AddInt(n)
	r.Count.Add(1)
}

func (r *Ratio) Hit() {
	r.Value.Add(1)
	r.Count.Add(1)
}

func (r *Ratio) Miss() {
	r.Count.Add(1)
}

func (r *Ratio) MarshalJSON() ([]byte, error) {
	if r.Count == 0 {
		return strJSON("NaN")
	}
	return strJSON(fmt.Sprintf("%.2f%%(%v/%v)",
		float64(r.Value*100)/float64(r.Count), r.Value, r.Count))
}

type Int int64

func (i *Int) AddInt(n int) int {
	return int(i.Add(int64(n)))
}

func (i *Int) Add(n int64) int64 {
	return atomic.AddInt64((*int64)(i), n)
}

// -----------------------------------------------------------------------------

type Size int64

func (s *Size) AddInt(n int) int {
	return (*Int)(s).AddInt(n)
}

func (s Size) Rate(d time.Duration) Rate {
	return Rate{s, d}
}

func (s *Size) Add(n int64) int64 {
	return (*Int)(s).Add(n)
}

func (i *Size) String() string {
	return Unit(atomic.LoadInt64((*int64)(i)))
}

func (i *Size) MarshalJSON() ([]byte, error) {
	return strJSON(i.String())
}

// -----------------------------------------------------------------------------

type Rate struct {
	Size     Size
	Duration time.Duration
}

func (i Rate) String() string {
	speed := Size(int64(float64(i.Size) / i.Duration.Seconds()))

	return fmt.Sprintf("%v in %v (%v/S)",
		i.Size.String(), i.Duration.String(),
		speed.String(),
	)
}
