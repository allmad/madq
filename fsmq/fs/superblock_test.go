package fs

import (
	"encoding/binary"
	"testing"

	"github.com/chzyer/fsmq/fsmq/util/qrand"
	"github.com/chzyer/fsmq/fsmq/util/test"
)

var root = test.Root("/test/fs/checkpoint")

func TestByteOrder(t *testing.T) {
	defer test.New(t)
	s := new(Superblock)
	currentByteOrder := s.ByteOrder()
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
	s.Metadata.Version = 1
	s.Metadata.CheckPoint = 2
	s.Metadata.BlockBit = 3
	s.SuperIndex[0].Key = int64(qrand.RandInt(1024))
	s.SuperIndex[BlockSize-1].Key = 4
	s.SuperIndex[BlockSize-1].Indirect = 5

	target := s.Encode()

	s2 := new(Superblock)
	test.Nil(s2.Decode(target))
	test.Equal(s.SuperblockV1, s2.SuperblockV1)
	test.Equal(s.SuperIndex[0].Key, s2.SuperIndex[0].Key)
}
