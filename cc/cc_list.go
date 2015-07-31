package cc

import "container/list"

type List struct {
	*list.List
}

func NewList() *List {
	return &List{list.New()}
}

func (l *List) All() []interface{} {
	ret := make([]interface{}, l.Len())
	idx := 0
	for i := l.Front(); i != nil; i = i.Next() {
		ret[idx] = i.Value
		idx++
	}
	return ret
}
