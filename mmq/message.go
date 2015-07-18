package mmq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/chzyer/mmq/internal/utils"

	"gopkg.in/logex.v1"
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

	ErrReseekable = []error{ErrMagicNotMatch, ErrChecksumNotMatch}
)

type HeaderBin [SizeMsgHeader]byte

const (
	MagicByte, MagicByteV2 byte = 0x9a, 0x80

	MaxMsgSize = math.MaxUint16

	SizeMsgMagic  = 2
	SizeMsgLength = 4
	SizeMsgId     = 8
	SizeMsgCrc    = 4
	SizeMsgVer    = 1

	SizeMsgHeader = SizeMsgMagic + SizeMsgLength

	OffsetMsgMagic  = 0
	OffsetMsgLength = SizeMsgMagic
	OffsetMsgId     = SizeMsgHeader
	OffsetMsgCrc    = OffsetMsgId + SizeMsgId

	OffsetMsgVer      = OffsetMsgCrc + SizeMsgCrc
	OffsetMsgCrcCheck = OffsetMsgVer
	OffsetMsgData     = OffsetMsgVer + SizeMsgVer
	OffsetMsgBody     = SizeMsgHeader

	MaxReseekBytes = math.MaxUint16 << 4
)

var (
	MagicBytes = []byte{MagicByte, MagicByteV2}
)

// in binary(magic always is 0x9a):
// +----------------+------------------------------+
// |     header     |             body             |
// +-------+--------+-------+-----+---------+------+
// | magic | length | msgid | crc | version | data |
// +-------+--------+-------+-----+---------+------+
// | 2     | 4      | 8     | 4   | 1       | ...  |
// +-------+--------+-------+-----+---------+------+
type Message struct {
	Length uint32

	MsgId   uint64
	Crc     uint32
	Version uint8
	Data    []byte

	underlay []byte
}

func NewMessageByData(data []byte) *Message {
	length := OffsetMsgData + len(data)
	underlay := make([]byte, length)
	copy(underlay[OffsetMsgData:], data)

	underlay[OffsetMsgVer] = byte(1)
	h := crc32.NewIEEE()
	h.Write(underlay[OffsetMsgVer:])

	m := &Message{
		Length:   uint32(length - OffsetMsgBody),
		Crc:      h.Sum32(),
		Version:  1,
		Data:     data,
		underlay: underlay,
	}

	copy(underlay, MagicBytes)
	binary.LittleEndian.PutUint32(underlay[OffsetMsgLength:], m.Length)
	// skip msg id
	binary.LittleEndian.PutUint32(underlay[OffsetMsgCrc:], m.Crc)
	return m
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
	logex.Errorf("begin to reseek, offset: (%v), why: (%v)", offset, err)

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

	content := make([]byte, SizeMsgHeader+int(length))
	if _, err = io.ReadFull(r, content[SizeMsgHeader:]); err != nil {
		return nil, logex.Trace(err)
	}
	copy(content, header)
	m, err := NewMessage(content)
	return m, logex.Trace(err)
}

func ReadMessageHeader(buf []byte, length *uint32) (err error) {
	if len(buf) < SizeMsgHeader {
		return ErrInvalidHeader.Format("short length")
	}
	if buf[0] != MagicByte || buf[1] != MagicByteV2 {
		return ErrMagicNotMatch.Trace(buf[:SizeMsgMagic])
	}

	*length = binary.LittleEndian.Uint32(buf[SizeMsgMagic:])
	return nil
}

func NewMessage(data []byte) (m *Message, err error) {
	if len(data) < OffsetMsgData {
		return nil, ErrInvalidHeader.Format("short length")
	}

	m = new(Message)
	if err := ReadMessageHeader(data[:SizeMsgHeader], &m.Length); err != nil {
		return nil, logex.Trace(err)
	}

	// body
	m.MsgId = binary.LittleEndian.Uint64(data[OffsetMsgId:])
	m.Crc = binary.LittleEndian.Uint32(data[OffsetMsgCrc:])

	buf := bytes.NewBuffer(data[OffsetMsgCrcCheck:])
	h := crc32.NewIEEE()
	h.Write(buf.Bytes())
	if m.Crc != h.Sum32() {
		return nil, ErrChecksumNotMatch.Trace(m.Crc, h.Sum32())
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
	m.Data = buf.Bytes()
	return nil
}

func (m *Message) SetMsgId(id uint64) {
	m.MsgId = id
	buf := m.underlay[OffsetMsgId:]
	binary.LittleEndian.PutUint64(buf, m.MsgId)
}

func (m *Message) WriteTo(w io.Writer) (int, error) {
	return w.Write(m.underlay)
}

func (m *Message) String() string {
	return fmt.Sprintf("%+v", *m)
}
