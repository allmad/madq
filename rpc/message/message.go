package message

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/chzyer/muxque/utils"
	"github.com/klauspost/crc32"
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
	ErrReadFlagInvalid  = logex.Define("set ReadFlag to RF_RESEEK_ON_FAULT required r is *cc.Reader")
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

type magicReader struct {
	eof bool
}

func (m *magicReader) Read(b []byte) (int, error) {
	if m.eof {
		return 0, io.EOF
	}
	n := copy(b, MagicBytes)
	m.eof = true
	return n, nil
}

func NewByBin(b []byte) *Ins {
	return NewByData(NewData(b))
}

func NewByData(data *Data) *Ins {
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

	copy(underlay, MagicBytes)
	binary.LittleEndian.PutUint32(underlay[OffsetMsgLength:], m.Length)
	binary.LittleEndian.PutUint64(underlay[OffsetMsgId:], 0)
	binary.LittleEndian.PutUint32(underlay[OffsetMsgCrc:], m.Crc)

	return m
}

func Read(reuseBuf *Header, reader io.Reader, rf ReadFlag) (*Ins, error) {
	n, msg, err := read(reuseBuf, reader)
	if rf != RF_RESEEK_ON_FAULT || !logex.EqualAny(err, ErrReseekable) {
		return msg, logex.Trace(err)
	}

	// reseekable and allow reseek on fault
	r, ok := reader.(*utils.Bufio)
	if !ok {
		panic(ErrReadFlagInvalid)
	}
	r.Offset(r.Offset(-1) - int64(n) + 1)
	offset := r.Offset(-1)
	logex.Errorf("begin to reseek, offset: (%v), why: (%v)", offset, err)

	for r.Offset(-1)-offset < MaxReseekBytes {
		_, err := r.ReadSlice(MagicByte)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			return nil, logex.Trace(err)
		}
		if errUnread := r.UnreadByte(); errUnread != nil {
			return nil, logex.Trace(err)
		}
		n, msg, err = read(reuseBuf, r)
		if !logex.EqualAny(err, ErrReseekable) {
			return msg, logex.Trace(err)
		}
		r.Offset(r.Offset(-1) - int64(n) + 1)
		continue
	}
	return nil, ErrReseekReachLimit.Trace()
}

func read(reuseBuf *Header, r io.Reader) (int, *Ins, error) {
	header := reuseBuf[:]
	var expectMsgId *uint64
	if r, ok := r.(*utils.Bufio); ok {
		off := uint64(r.Offset(-1))
		expectMsgId = &off
	}

	n, err := io.ReadFull(r, header)
	nRead := n
	if err != nil {
		return n, nil, logex.Trace(err)
	}

	var length uint32
	if err := ReadHeader(header, &length); err != nil {
		return n, nil, logex.Trace(err)
	}

	content := make([]byte, SizeMsgHeader+int(length))
	n, err = io.ReadFull(r, content[SizeMsgHeader:])
	nRead += n
	if err != nil {
		return nRead, nil, logex.Trace(err)
	}

	copy(content, header)
	m, err := New(content)
	if err != nil {
		return nRead, nil, logex.Trace(err)
	}
	if expectMsgId != nil && m.MsgId != *expectMsgId {
		return nRead, nil, ErrMsgIdNotMatch.Trace(*expectMsgId, m.MsgId)
	}
	return nRead, m, nil
}

func ReadHeader(buf []byte, lengthRef *uint32) (err error) {
	if len(buf) < SizeMsgHeader {
		return ErrInvalidLength.Trace(len(buf))
	}
	if buf[0] != MagicByte || buf[1] != MagicByteV2 {
		return ErrMagicNotMatch.Trace(buf[:SizeMsgMagic])
	}

	length := binary.LittleEndian.Uint32(buf[SizeMsgMagic:])
	if length < MinMsgLength {
		return ErrInvalidLength.Trace(length, SizeMsgHeader)
	}
	*lengthRef = length
	return nil
}

func New(data []byte) (m *Ins, err error) {
	if len(data) < OffsetMsgData {
		return nil, ErrInvalidHeader.Format("short length")
	}

	m = &Ins{
		underlay: data,
	}
	if err := ReadHeader(data[:SizeMsgHeader], &m.Length); err != nil {
		return nil, logex.Trace(err)
	}

	// body
	m.MsgId = binary.LittleEndian.Uint64(data[OffsetMsgId:])
	m.Crc = binary.LittleEndian.Uint32(data[OffsetMsgCrc:])

	h := crc32.NewIEEE()
	h.Write(data[OffsetMsgCrcCheck:])
	if m.Crc != h.Sum32() {
		logex.Info(data)
		return nil, ErrChecksumNotMatch.Trace(m.Crc, h.Sum32())
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

func (m *Ins) NextOff() int64 {
	return int64(len(m.underlay)) + int64(m.MsgId)
}

func (m *Ins) Size() int {
	return len(m.underlay)
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

func NewData(b []byte) *Data {
	m := &Data{
		underlay: make([]byte, len(b)+OffsetMsgData),
	}
	copy(m.underlay[OffsetMsgData:], b)
	return m
}

func (m *Data) Bytes() []byte {
	return m.underlay[OffsetMsgData:]
}
