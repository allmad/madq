package message

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/chzyer/muxque/utils"

	"gopkg.in/logex.v1"
)

func BenchmarkMarshal(b *testing.B) {
	source := NewData([]byte(utils.RandString(256)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewByData(source)
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	m := NewByData(NewData([]byte(utils.RandString(256))))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(m.underlay)
	}
}

func BenchmarkRead(b *testing.B) {
	m := NewByData(NewData([]byte(utils.RandString(256))))
	buf := bytes.NewBuffer(make([]byte, 0, m.Size()*b.N))
	for i := 0; i < b.N; i++ {
		m.WriteTo(buf)
	}
	b.ResetTimer()
	var header Header
	for i := 0; i < b.N; i++ {
		_, err := Read(&header, buf, RF_RESEEK_ON_FAULT)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadDisk(b *testing.B) {
	m := NewByData(NewData([]byte(utils.RandString(256))))
	buf := bytes.NewBuffer(make([]byte, 0, m.Size()*b.N))
	for i := 0; i < b.N; i++ {
		m.WriteTo(buf)
	}
	path := utils.GetRoot("/test/message/tmp")
	os.MkdirAll(filepath.Dir(path), 0777)
	if err := ioutil.WriteFile(path, buf.Bytes(), 0666); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	f, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var header Header
	for i := 0; i < b.N; i++ {
		_, err := Read(&header, r, RF_RESEEK_ON_FAULT)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestMessage(t *testing.T) {
	m := NewByData(NewData([]byte("hello")))
	{
		m2, err := Decode(m.underlay)
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
		m3, err := Read(&header, utils.NewReaderBlock(m.underlay), RF_DEFAULT)
		if err != nil {
			logex.Error(err)
			t.Fatal(err)
		}
		if !reflect.DeepEqual(m, m3) {
			logex.Struct(m, m3)
			t.Fatal("result not expect")
		}
	}

	{ // deal with `hello`{message}
		prefix := []byte("hello")
		m.SetMsgId(uint64(len(prefix)))
		buf := utils.NewReaderBlock(append(prefix, m.underlay...))
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
		buf := utils.NewReaderBlock(bin)
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
