package message

import (
	"reflect"
	"testing"

	"github.com/chzyer/muxque/utils"

	"gopkg.in/logex.v1"
)

func BenchmarkNewMessageByData(b *testing.B) {
	source := NewData([]byte(utils.RandString(256)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewByData(source)
	}
}

func BenchmarkNewMessageRaw256(b *testing.B) {
	m := NewByData(NewData([]byte(utils.RandString(256))))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(m.underlay)
	}
}

func TestMessage(t *testing.T) {
	m := NewByData(NewData([]byte("hello")))
	{
		m2, err := New(m.underlay)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(m, m2) {
			logex.Struct(m, m2)
			t.Fatal("result not expect")
		}
	}
	var header Header
	{
		m3, err := Read(&header, utils.NewReaderBuf(m.underlay), RF_DEFAULT)
		if err != nil {
			logex.Error(err)
			t.Fatal(err)
		}
		if !reflect.DeepEqual(m, m3) {
			logex.Struct(m, m3)
			t.Fatal("result not expect")
		}
	}

	{
		prefix := []byte("hello")
		m.SetMsgId(uint64(len(prefix)))
		buf := utils.NewReaderBuf(append(prefix, m.underlay...))
		m4, err := Read(&header, buf, RF_RESEEK_ON_FAULT)
		if err != nil {
			logex.Error(err)
			t.Fatal(err)
		}
		if !reflect.DeepEqual(m, m4) {
			logex.Struct(m, m4)
			t.Fatal("result not expect")
		}
		m.SetMsgId(0)
	}

	{
		// dump data
		bin := append([]byte("hello"), MagicBytes...)
		bin = append(bin, []byte{4, 0, 0, 0}...)

		m.SetMsgId(uint64(len(bin)))
		bin = append(bin, m.underlay...)
		buf := utils.NewReaderBuf(bin)
		m5, err := Read(&header, buf, RF_RESEEK_ON_FAULT)
		if err != nil {
			logex.Error(err)
			t.Fatal(err)
		}
		if !reflect.DeepEqual(m, m5) {
			logex.Struct(m, m5)
			t.Fatal("result not expect")
		}
		m.SetMsgId(0)
	}

}
