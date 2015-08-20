package utils

import (
	"reflect"
	"testing"
)

func TestList(t *testing.T) {
	l := NewList()
	l.PushBack(1)
	if !reflect.DeepEqual(l.All(), []interface{}{1}) {
		t.Fatal("result not expected")
	}
}
