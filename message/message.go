package message

import (
	"bufio"
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
	ErrInvalidLength    = logex.Define("invalid message: length short")
	ErrChecksumNotMatch = logex.Define("message checksum not match")
	ErrMessageTooLarge  = logex.Define("message size exceed limit")
	ErrReadFlagInvalid  = logex.Define("set ReadFlag to RF_RESEEK_ON_FAULT required r is *utils.Reader")
	ErrReseekReachLimit = logex.Define("reseek reach size limit")
	ErrMsgIdNotMatch    = logex.Define("message id not match as expect")

	ErrReseekable = []error{
		ErrMagicNotMatch, ErrChecksumNotMatch, ErrInvalidLength,
	}
)

type Header [SizeMsgHeader]byte

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
	MinMsgLength   = SizeMsgId + SizeMsgCrc + SizeMsgVer
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
type Ins struct {
	Length uint32

	MsgId   uint64
	Crc     uint32
	Version uint8
	Data    []byte

	underlay []byte
}

type ReplyCtx struct {
	Topic string
	Msgs  []*Ins
}

func NewReplyCtx(name string, msgs []*Ins) *ReplyCtx {
	return &ReplyCtx{name, msgs}
}

func NewMessageByData(data *Data) *Ins {
	underlay := data.underlay
	underlay[OffsetMsgVer] = byte(1)
	h := crc32.NewIEEE()
	h.Write(underlay[OffsetMsgVer:])

	m := &Ins{
		Length:   uint32(len(underlay) - OffsetMsgBody),
		Crc:      h.Sum32(),
		Version:  1,
		Data:     data.Bytes(),
		underlay: underlay,
	}
	logex.Info(data.Bytes())

	copy(underlay, MagicBytes)
	binary.LittleEndian.PutUint32(underlay[OffsetMsgLength:], m.Length)
	binary.LittleEndian.PutUint64(underlay[OffsetMsgId:], 0)
	binary.LittleEndian.PutUint32(underlay[OffsetMsgCrc:], m.Crc)

	return m
}

func ReadMessage(reuseBuf *Header, reader io.Reader, rf ReadFlag) (*Ins, error) {
	checksum := true
	n, msg, err := readMessage(reuseBuf, reader, checksum)
	if rf != RF_RESEEK_ON_FAULT || !logex.EqualAny(err, ErrReseekable) {
		return msg, logex.Trace(err)
	}

	// reseekable and allow reseek on fault
	r, ok := reader.(*utils.Reader)
	if !ok {
		panic(ErrReadFlagInvalid)
	}
	r.Offset -= int64(n)
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
		n, msg, err = readMessage(reuseBuf, r, checksum)
		if !logex.EqualAny(err, ErrReseekable) {
			return msg, logex.Trace(err)
		}
		r.Offset -= int64(n)
		buf = bufio.NewReader(r)
		continue
	}
	return nil, ErrReseekReachLimit.Trace()
}

func readMessage(reuseBuf *Header, r io.Reader, checksum bool) (int, *Ins, error) {
	header := reuseBuf[:]
	var expectMsgId *uint64
	if r, ok := r.(*utils.Reader); ok {
		off := uint64(r.Offset)
		expectMsgId = &off
	}

	n, err := io.ReadFull(r, header)
	nRead := n
	if err != nil {
		return n, nil, logex.Trace(err)
	}

	var length uint32
	if err := ReadMessageHeader(header, &length); err != nil {
		return n, nil, logex.Trace(err)
	}

	content := make([]byte, SizeMsgHeader+int(length))
	n, err = io.ReadFull(r, content[SizeMsgHeader:])
	nRead += n
	if err != nil {
		return nRead, nil, logex.Trace(err)
	}

	copy(content, header)
	m, err := NewMessage(content, checksum)
	if err != nil {
		return nRead, nil, logex.Trace(err)
	}
	if expectMsgId != nil && m.MsgId != *expectMsgId {
		return nRead, nil, ErrMsgIdNotMatch.Trace(*expectMsgId, m.MsgId)
	}
	return nRead, m, nil
}

func ReadMessageHeader(buf []byte, lengthRef *uint32) (err error) {
	if len(buf) < SizeMsgHeader {
		return ErrInvalidLength.Trace(len(buf))
	}
	if buf[0] != MagicByte || buf[1] != MagicByteV2 {
		return ErrMagicNotMatch.Trace(buf[:SizeMsgMagic])
	}

	length := binary.LittleEndian.Uint32(buf[SizeMsgMagic:])
	if length < MinMsgLength {
		return ErrInvalidLength.Trace(length)
	}
	*lengthRef = length
	return nil
}

func NewMessage(data []byte, checksum bool) (m *Ins, err error) {
	if len(data) < OffsetMsgData {
		return nil, ErrInvalidHeader.Format("short length")
	}

	m = &Ins{
		underlay: data,
	}
	if err := ReadMessageHeader(data[:SizeMsgHeader], &m.Length); err != nil {
		return nil, logex.Trace(err)
	}

	// body
	m.MsgId = binary.LittleEndian.Uint64(data[OffsetMsgId:])
	m.Crc = binary.LittleEndian.Uint32(data[OffsetMsgCrc:])

	if checksum {
		h := crc32.NewIEEE()
		h.Write(data[OffsetMsgCrcCheck:])
		if m.Crc != h.Sum32() {
			return nil, ErrChecksumNotMatch.Trace(m.Crc, h.Sum32())
		}
	}

	m.Version = uint8(data[OffsetMsgVer])
	switch m.Version {
	case 1:
		m.Data = data[OffsetMsgData:]
	default:
		return nil, ErrInvalidMessage.Format("unsupport version")
	}

	return m, nil
}

func (m *Ins) Bytes() []byte {
	return m.underlay
}

func (m *Ins) SetMsgId(id uint64) {
	m.MsgId = id
	buf := m.underlay[OffsetMsgId:]
	binary.LittleEndian.PutUint64(buf, m.MsgId)
}

func (m *Ins) WriteTo(w io.Writer) (int, error) {
	return w.Write(m.underlay)
}

func (m *Ins) String() string {
	return fmt.Sprintf("%+v", *m)
}

type Data struct {
	underlay []byte
}

func NewMessageData(b []byte) *Data {
	m := &Data{
		underlay: make([]byte, len(b)+OffsetMsgData),
	}
	copy(m.underlay[OffsetMsgData:], b)
	return m
}

func (m *Data) Bytes() []byte {
	return m.underlay[OffsetMsgData:]
}
