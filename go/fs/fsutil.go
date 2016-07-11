package fs

import (
	"fmt"

	"github.com/allmad/madq/go/bio"
	"github.com/allmad/madq/go/common"
)

const (
	BlockBit  = 18
	BlockSize = 1 << BlockBit
)

func MakeRoom(b []byte, n int) []byte {
	if n <= cap(b)-len(b) {
		return b[:n]
	}

	newBuf := make([]byte, n+1, 2*n)
	copy(newBuf, b)
	return newBuf
}

func GetInodeIdx(offset int64) int32 {
	blkOff := offset >> BlockBit
	return int32(blkOff / InodeBlockCnt)
}

func GetBlockIdx(offset int64) int32 {
	return int32(offset >> BlockBit)
}

// n: append data size
func CalNeedInodeCnt(ino *Inode, n int) int {
	if int(InodeCap-ino.Size) > n {
		return 1
	}
	n -= int(InodeCap - ino.Size)
	if n%InodeCap == 0 {
		return (n / InodeCap) + 1
	}
	return (n / InodeCap) + 2
}

func GetBlockCnt(n int) int {
	ret := n >> BlockBit
	if n&(BlockSize-1) == 0 {
		return ret
	}
	return ret + 1
}

func FloorBlk(n int) int {
	if n&(BlockSize-1) == 0 {
		return n
	}
	return ((n >> BlockBit) + 1) << BlockBit
}

func initOffsetIdx() (ret [32]int) {
	factor := 1
	base := 0
	for idx := 1; idx < len(ret); {
		for i := 0; i < factor/2; i++ {
			ret[idx] = base - 1
			idx++
		}
		base++
		factor *= 2
	}
	return
}

// -----------------------------------------------------------------------------

type VolumeSource struct {
	flock *common.Flock
	file  *bio.File
	*bio.Hybrid
}

func (v *VolumeSource) Close() {
	v.flock.Unlock()
	v.file.Close()
}

func NewVolumeSource(dir string) (*VolumeSource, error) {
	flock, err := common.LockDir(dir)
	if err != nil {
		return nil, err
	}

	file, err := bio.NewFile(dir)
	if err != nil {
		return nil, fmt.Errorf("open volume: ", err)
	}

	return &VolumeSource{
		flock:  flock,
		file:   file,
		Hybrid: bio.NewHybrid(file),
	}, nil
}
