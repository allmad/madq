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

	MsgIdOffset    = 10
	MessageMaxSize = math.MaxUint32

	MessageMagicSize      = 1
	MessageLengthSize     = 4
	MessageHeaderSize     = MessageMagicSize + MessageLengthSize
	MessageMaxContentSzie = MessageMaxSize - 18
)

var (
	ErrMagicNotMatch   = logex.Define("magic byte not match")
	ErrInvalidMessage  = logex.Define("invalid message: %v")
	ErrInvalidHeader   = logex.Define("invalid message header: %v")
	ErrMessageTooLarge = logex.Define("message size exceed limit")
)

// in binary(magic always is 0x9a):
// +-------+--------+-----+---------+-------+------+
// | magic | length | crc | version | msgid | data |
// +-------+--------+-----+---------+-------+------+
// | 1     | 4      | 4   | 1       | 8     | ...  |
// +-------+--------+-----+---------+-------+------+
type Message struct {
	Length  uint32
	Crc     uint32
	Version uint8

	MsgId    uint64
	Data     []byte
	underlay []byte
}

type ReadFlag int

const (
	F_DEFAULT     ReadFlag = 0
	F_ALLOW_FAULT          = 1
)

func ReadMessage(r io.Reader, flag ReadFlag) (*Message, error) {
	header := make([]byte, MessageHeaderSize)
	_, err := io.ReadFull(r, header)
	if err != nil {
		return nil, logex.Trace(err)
	}

	var length uint32
	if err := ReadMessageHeader(header, &length); err != nil {
		if logex.Equal(err, ErrMagicNotMatch) && flag == F_ALLOW_FAULT {
			// retry
		}
		return nil, logex.Trace(err)
	}

	content := make([]byte, MessageHeaderSize+int(length))
	if _, err = io.ReadFull(r, content[5:]); err != nil {
		return nil, logex.Trace(err)
	}
	copy(content, header)
	m, err := NewMessage(content)
	return m, logex.Trace(err)
}

func ReadMessageHeader(buf []byte, length *uint32) (err error) {
	if len(buf) < MessageHeaderSize {
		return ErrInvalidHeader.Format("short length")
	}
	magic := buf[0]
	if magic != MagicByte {
		return ErrMagicNotMatch.Trace()
	}

	*length = binary.LittleEndian.Uint32(buf[1:5])
	return nil
}

func NewMessage(data []byte) (m *Message, err error) {
	if len(data) < MessageHeaderSize+4 { // 1 + 4 + 4
		return nil, ErrInvalidHeader.Format("short length")
	}

	m = new(Message)
	if err := ReadMessageHeader(data[:5], &m.Length); err != nil {
		return nil, logex.Trace(err)
	}
	m.Crc = binary.LittleEndian.Uint32(data[5:9])

	buf := bytes.NewBuffer(data[9:])

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
