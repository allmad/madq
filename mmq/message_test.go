package mmq

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/chzyer/mmq/internal/utils"

	"gopkg.in/logex.v1"
)

func BenchmarkNewMessageByData(b *testing.B) {
	source := []byte(utils.RandString(256))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewMessageByData(source)
	}
}

func BenchmarkNewMessageRaw256(b *testing.B) {
	m := NewMessageByData([]byte(utils.RandString(256)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewMessage(m.underlay, false)
	}
}

func TestMessage(t *testing.T) {
	m := NewMessageByData([]byte("hello"))
	m2, err := NewMessage(m.underlay, true)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(m, m2) {
		logex.Struct(m, m2)
		t.Error("result not expect")
	}
	var header HeaderBin
	m3, err := ReadMessage(&header, bytes.NewReader(m.underlay), RF_DEFAULT)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m, m3) {
		logex.Struct(m, m3)
		t.Error("result not expect")
	}

	{
		buf := utils.NewReaderBuf(append([]byte("hello"), m.underlay...))
		m4, err := ReadMessage(&header, buf, RF_RESEEK_ON_FAULT)
		if err != nil {
			logex.Error(err)
			t.Fatal(err)
		}
		if !reflect.DeepEqual(m, m4) {
			logex.Struct(m, m4)
			t.Error("result not expect")
		}
	}

	{
		bin := append([]byte("hello"), MagicBytes...)
		bin = append(bin, []byte{4, 0, 0, 0}...)
		bin = append(bin, m.underlay...)
		buf := utils.NewReaderBuf(bin)
		m5, err := ReadMessage(&header, buf, RF_RESEEK_ON_FAULT)
		if err != nil {
			logex.Error(err)
			t.Fatal(err)
		}
		if !reflect.DeepEqual(m, m5) {
			logex.Struct(m, m5)
			t.Error("result not expect")
		}
	}

}
