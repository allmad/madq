package mmq

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/chzyer/mmq/internal/utils"

	"hash/crc32"

	"gopkg.in/logex.v1"
)

var (
	MessageMaxSize = math.MaxUint16
)

type Message struct {
	Version int16
	Length  int32
	MsgId   uint16
	Crc     uint32
	Data    []byte
	buffer  bytes.Buffer
}

func (m *Message) getCrc() uint32 {
	if m.Crc == 0 {
		n := crc32.NewIEEE()
		n.Write(m.Data)
		m.Crc = n.Sum32()
	}
	return m.Crc
}

func NewMessage(r io.Reader) (m *Message, err error) {
	var version int16
	if err := binary.Read(w, binary.LittleEndian, &version); err != nil {
		return logex.Trace(err)
	}
	switch version {
	case 1:
		return NewMessageV1(r), nil
	}
	return nil, nil
}

func NewMessageV1(r io.Reader) (m *Message, err error) {
	m = &Message{
		Version: 1,
		buffer:  bytes.NewBuffer(make([]byte, 0, 512)),
	}
	buffer := io.TeeReader(r, m.buffer)
	if err := binary.Read(buffer, binary.LittleEndian, &m.Length); err != nil {
		return logex.Trace(err, "length")
	}

	return
}

func (m *Message) WriteTo(w io.Writer) (err error) {
	return logex.Trace(utils.BinaryWriteMulti(w, []interface{}{
		m.Version,
		m.Length,
		m.getCrc(),
		m.Data,
	}))
}
