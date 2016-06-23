package fs

import (
	"encoding/json"

	"github.com/chzyer/madq/go/ptrace"
)

var Stat GStat

type GStat struct {
	Flusher struct {
		BlockCopy ptrace.Size
	}
	Inode struct {
		Cache struct {
			NextHit   ptrace.Ratio
			InoIdxHit ptrace.Ratio
		}

		ReadDisk    ptrace.Int
		PrevSeekCnt ptrace.Ratio
	}
}

func (c *GStat) String() string {
	ret, _ := json.MarshalIndent(c, "", "\t")
	return string(ret)
}
