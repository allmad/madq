package rpc

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/chzyer/muxque/rpc/message"
)

func notexpect(t *testing.T) {
	t.Fatal("result not expected")
}

func TestProtStruct(t *testing.T) {
	subobj := NewInt64(100)
	obj := NewStruct(subobj)
	buf := bytes.NewBuffer(nil)
	if err := WriteItem(buf, obj); err != nil {
		t.Fatal(err)
	}
	var subobj2 Int64
	obj2 := NewStruct(&subobj2)
	if err := obj2.PSet(buf); err != nil {
		t.Fatal(err)
	}
	if subobj2.Int() != subobj.Int() {
		notexpect(t)
	}

}

func TestProtMsgs(t *testing.T) {
	msgs := []*message.Ins{message.NewByBin([]byte("hello"))}
	obj := NewMsgs(msgs)
	buf := bytes.NewBuffer(nil)
	if err := WriteItem(buf, obj); err != nil {
		t.Fatal(err)
	}
	var header message.Header
	obj2, err := ReadMsgs(&header, buf)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(obj.Msgs(), obj2.Msgs()) {
		notexpect(t)
	}
	buf.Reset()
	if err := WriteItem(buf, obj2); err != nil {
		t.Fatal(err)
	}
	obj.underlay = nil
	if err := obj.PSet(buf); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(obj.underlay, obj2.underlay) {
		notexpect(t)
	}
}

func TestProtError(t *testing.T) {
	obj := NewError(nil)
	if obj.Err() != nil {
		notexpect(t)
	}
	buf := bytes.NewBuffer(nil)
	if err := WriteItem(buf, obj); err != nil {
		t.Fatal(err)
	}
	obj2, err := ReadError(buf)
	if err != nil {
		t.Fatal(err)
	}
	if obj2.Err() != obj.Err() {
		notexpect(t)
	}
	buf.Reset()
	obj3 := NewError(errors.New("hello"))
	if err := WriteItem(buf, obj3); err != nil {
		t.Fatal(err)
	}
	if err := obj.PSet(buf); err != nil {
		t.Fatal(err)
	}
	if obj.Err() == obj3.Err() {
		notexpect(t)
	}
	if obj.Err().Error() != obj3.Err().Error() {
		notexpect(t)
	}

}

func TestProtString(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	obj := NewString("hello")
	if err := WriteItem(buf, obj); err != nil {
		t.Fatal(err)
	}
	obj2, err := ReadString(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(obj.underlay, obj2.underlay) {
		t.Fatal("result not expected")
	}
	obj.underlay = []byte("cao")
	buf.Reset()
	if err := WriteItem(buf, obj2); err != nil {
		t.Fatal(err)
	}
	if err := obj.PSet(buf); err != nil {
		t.Fatal(err)
	}
	if obj.String() != string(obj2.Bytes()) {
		t.Fatal("result not expected")
	}
}

func TestProtInt64(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	obj := NewInt64(123)
	if err := WriteItem(buf, obj); err != nil {
		t.Fatal(err)
	}
	obj2, err := ReadInt64(buf)
	if err != nil {
		t.Fatal(err)
	}
	if obj.underlay != obj2.underlay {
		t.Fatal("result not expected")
	}
	obj.underlay = 111
	buf.Reset()
	if err := WriteItem(buf, obj2); err != nil {
		t.Fatal(err)
	}
	if err := obj.PSet(buf); err != nil {
		t.Fatal(err)
	}
	if int64(obj.Int()) != obj2.Int64() {
		t.Fatal("result not expected")
	}
}
