package mmq

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"

	"gopkg.in/logex.v1"
)

const (
	MagicByte byte = 0x9a

	MsgIdOffset           = 10
	MessageMaxSize        = math.MaxUint16
	MessageMaxContentSzie = MessageMaxSize - 18
)

var (
	ErrMagicNotMatch   = logex.Define("magic byte not match")
	ErrInvalidMessage  = logex.Define("invalid message: %v")
	ErrMessageTooLarge = logex.Define("message size exceed limit")
)

// in binary(magic always is 0x9a):
// +-------+--------+-----+---------+-------+------+
// | magic | length | crc | version | msgid | data |
// +-------+--------+-----+---------+-------+------+
// | 1     | 4      | 4   | 1       | 8     | ...  |
// +-------+--------+-----+---------+-------+------+
type Message struct {
	Length  int32
	Crc     uint32
	Version int16

	MsgId    uint64
	Data     []byte
	underlay []byte
}

func NewMessage(data []byte) (*Message, error) {
	buf := bytes.NewBuffer(data)
	magic, err := buf.ReadByte()
	if err != nil {
		return nil, logex.Trace(err, "magic")
	}
	if magic != MagicByte {
		return nil, ErrMagicNotMatch
	}
	m := new(Message)
	if err = binary.Read(buf, binary.LittleEndian, &m.Length); err != nil {
		return nil, ErrInvalidMessage.Format("read length").Follow(err)
	}
	if err = binary.Read(buf, binary.LittleEndian, &m.Crc); err != nil {
		return nil, ErrInvalidMessage.Format("read crc").Follow(err)
	}
	h := crc32.NewIEEE()
	h.Write(buf.Bytes())
	if m.Crc != h.Sum32() {
		return nil, ErrInvalidMessage.Format("crc not match")
	}
	if err = binary.Read(buf, binary.LittleEndian, &m.Version); err != nil {
		return nil, ErrInvalidMessage.Format("read version")
	}
	switch m.Version {
	case 1:
		err = parseMsgV1(m, buf)
	default:
		err = ErrInvalidMessage.Format("unsupport version")
	}
	if err != nil {
		return nil, logex.Trace(err)
	}

	return m, nil
}

func parseMsgV1(m *Message, buf *bytes.Buffer) error {
	if err := binary.Read(buf, binary.LittleEndian, &m.MsgId); err != nil {
		return ErrInvalidMessage.Format("read msgid")
	}
	return nil
}

func (m *Message) SetMsgId(id uint64) {
	m.MsgId = id
	buf := m.Data[MsgIdOffset : MsgIdOffset+8]
	binary.LittleEndian.PutUint64(buf, m.MsgId)
}

func (m *Message) WriteTo(w io.Writer) (int, error) {
	return w.Write(m.underlay)
}
