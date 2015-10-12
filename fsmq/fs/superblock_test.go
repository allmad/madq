package fs

import (
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"

	"github.com/chzyer/fsmq/fsmq/util/qrand"
	"github.com/chzyer/fsmq/fsmq/util/test"
)

var root = test.Root("/test/fs/checkpoint")

func TestCheckSizeV1(t *testing.T) {
	defer test.New(t)
	s := SuperblockV1{}
	size := int(unsafe.Sizeof(s))
	test.Equal(size, ReserveBlockSize)

	size = int(unsafe.Sizeof(s) - unsafe.Sizeof(s.IndirectAddr))
	test.Equal(size, MetaSize)

	size = int(unsafe.Sizeof(InodeIndex{}))
	test.Equal(size, 32)

	maxRecordNum := len(s.IndirectAddr) * BlockSize / size
	test.Equal(maxRecordNum < 1<<20, false)
	test.Equal(maxRecordNum > 5<<20, false, fmt.Errorf("%v", maxRecordNum))
}

func TestByteOrder(t *testing.T) {
	defer test.New(t)
	s := new(Superblock)
	currentByteOrder := s.GetByteOrder()
	wrongByteOrder := LittleEndian
	if wrongByteOrder == currentByteOrder {
		wrongByteOrder = BigEndian
	}

	b := make([]byte, s.Size())
	currentByteOrder.Binary().PutUint64(b, uint64(currentByteOrder))
	currentByteOrder.Binary().PutUint64(b[8:], 1)
	test.Nil(s.Decode(b))

	test.Equal(s.Decode(b[:16]), ErrSbInvalid)
	test.Equal(s.Decode(b[:15]), ErrSbInvalidLen)

	binary.LittleEndian.PutUint64(b[8:], 0)
	test.Equal(s.Decode(b), ErrSbInvalidVersion)

	binary.LittleEndian.PutUint64(b, uint64(wrongByteOrder))
	test.Equal(s.Decode(b), ErrByteOrderNotEqual)

	LittleEndian.String()
	BigEndian.String()
	ByteOrder(-1).String()
	wrongByteOrder.Binary()
}

func TestEncodeDecode(t *testing.T) {
	defer test.New(t)
	s := new(Superblock)
	s.SuperblockV1 = new(SuperblockV1)
	s.Version = 1
	s.CheckPoint = 2
	s.BlockBit = 3
	s.IndirectAddr[0] = int64(qrand.RandInt(1024))
	s.IndirectAddr[BlockSize-1] = 5

	target := s.Encode()

	s2 := new(Superblock)
	test.Nil(s2.Decode(target))
	test.Equal(s.SuperblockV1, s2.SuperblockV1)
	test.Equal(s.IndirectAddr[0], s2.IndirectAddr[0])
}
