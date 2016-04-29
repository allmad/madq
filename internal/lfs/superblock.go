package lfs

import "encoding/binary"

// 1 block
type Superblock struct {
	Version    int32
	Checkpoint int64
	padding    [BlockSize - 8 - 4]byte
}

func (s *Superblock) Unmarshal(b []byte) error {
	s.Version = int32(binary.BigEndian.Uint32(b[:4]))
	s.Checkpoint = int64(binary.BigEndian.Uint64(b[4:12]))
	return nil
}

func (s *Superblock) Marshal() ([]byte, error) {
	ret := make([]byte, BlockSize)
	binary.BigEndian.PutUint32(ret[:4], uint32(s.Version))
	binary.BigEndian.PutUint64(ret[4:12], uint64(s.Checkpoint))
	return ret, nil
}
