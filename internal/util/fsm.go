package util

import "sync/atomic"

type State int32

func (i *State) Set(val State) bool {
	if val == 0 {
		return false
	}
	prev := int32(val - 1)
	return atomic.CompareAndSwapInt32((*int32)(i), prev, int32(val))
}

func (i *State) After(v State) bool {
	return atomic.LoadInt32((*int32)(i)) >= int32(v)
}
