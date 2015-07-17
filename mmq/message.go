package mmq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"

	"github.com/chzyer/mmq/internal/utils"

	"gopkg.in/logex.v1"
)

const (
	MagicByte, MagicByteV2 byte = 0x9a, 0x80

	MessageMaxSize = math.MaxUint16

	MessageMagicSize   = 2
	MessageLengthSize  = 4
	MessageCrcSize     = 4
	MessageVersionSize = 1
	MessageMsgIdSize   = 8

	MessageHeaderSize     = MessageMagicSize + MessageLengthSize
	MessageBodyOffset     = MessageHeaderSize + MessageCrcSize
	MessageMsgIdOffset    = MessageBodyOffset + MessageVersionSize
	MessageMaxContentSize = MessageMaxSize - MessageMsgIdOffset - MessageMsgIdSize

	MaxReseekBytes = math.MaxUint16 << 4
)

type ReadFlag int

const (
	_                  = ReadFlag(iota)
	RF_DEFAULT         = 0
	RF_RESEEK_ON_FAULT = 1
)

var (
	ErrMagicNotMatch    = logex.Define("magic byte not match")
	ErrInvalidMessage   = logex.Define("invalid message: %v")
	ErrInvalidHeader    = logex.Define("invalid message header: %v")
	ErrChecksumNotMatch = logex.Define("message checksum not match")
	ErrMessageTooLarge  = logex.Define("message size exceed limit")
	ErrReadFlagInvalid  = logex.Define("set ReadFlag to RF_RESEEK_ON_FAULT required r is *utils.Reader")
	ErrReseekReachLimit = logex.Define("reseek reach size limit")
)

var (
	ErrReseekable = []error{ErrMagicNotMatch, ErrChecksumNotMatch}
)

type HeaderBin [MessageHeaderSize]byte

// in binary(magic always is 0x9a):
// +-------+--------+-----+---------+-------+------+
// | magic | length | crc | version | msgid | data |
// +-------+--------+-----+---------+-------+------+
// | 2     | 4      | 4   | 1       | 8     | ...  |
// +-------+--------+-----+---------+-------+------+
type Message struct {
	Length  uint32
	Crc     uint32
	Version uint8

	MsgId    uint64
	Data     []byte
	underlay []byte
}

func ReadMessage(reuseBuf *HeaderBin, reader io.Reader, rf ReadFlag) (*Message, error) {
	msg, err := readMessage(reuseBuf, reader)
	if rf != RF_RESEEK_ON_FAULT || !logex.EqualAny(err, ErrReseekable) {
		return msg, logex.Trace(err)
	}
	// TODO: reseek must backto the offset of magicByte
	// reseekable and allow reseek on fault
	r, ok := reader.(*utils.Reader)
	if !ok {
		panic(ErrReadFlagInvalid)
	}
	offset := r.Offset
	var skiped int64

	buf := bufio.NewReader(r)
	for skiped < MaxReseekBytes {
		tmp, err := buf.ReadSlice(MagicByte)
		skiped += int64(len(tmp))
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			return nil, logex.Trace(err)
		}

		offset = offset + skiped - 1
		r.Offset = offset
		msg, err = readMessage(reuseBuf, r)
		if !logex.EqualAny(err, ErrReseekable) {
			return msg, logex.Trace(err)
		}

		continue
	}
	return nil, ErrReseekReachLimit.Trace()
}

func readMessage(reuseBuf *HeaderBin, r io.Reader) (*Message, error) {
	header := reuseBuf[:]
	_, err := io.ReadFull(r, header)
	if err != nil {
		return nil, logex.Trace(err)
	}

	var length uint32
	if err := ReadMessageHeader(header, &length); err != nil {
		return nil, logex.Trace(err)
	}

	content := make([]byte, MessageHeaderSize+int(length))
	if _, err = io.ReadFull(r, content[MessageHeaderSize:]); err != nil {
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
	if buf[0] != MagicByte || buf[1] != MagicByteV2 {
		return ErrMagicNotMatch.Trace()
	}

	buf = buf[MessageMagicSize : MessageMagicSize+MessageLengthSize]
	*length = binary.LittleEndian.Uint32(buf)
	return nil
}

func NewMessage(data []byte) (m *Message, err error) {
	if len(data) < MessageHeaderSize+MessageLengthSize {
		return nil, ErrInvalidHeader.Format("short length")
	}

	m = new(Message)
	if err := ReadMessageHeader(data[:MessageHeaderSize], &m.Length); err != nil {
		return nil, logex.Trace(err)
	}
	m.Crc = binary.LittleEndian.Uint32(data[MessageHeaderSize:MessageBodyOffset])

	buf := bytes.NewBuffer(data[MessageBodyOffset:])

	h := crc32.NewIEEE()
	h.Write(buf.Bytes())
	if m.Crc != h.Sum32() {
		return nil, ErrChecksumNotMatch.Trace()
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
	buf := m.Data[MessageMsgIdOffset : MessageMsgIdOffset+8]
	binary.LittleEndian.PutUint64(buf, m.MsgId)
}

func (m *Message) WriteTo(w io.Writer) (int, error) {
	return w.Write(m.underlay)
}
