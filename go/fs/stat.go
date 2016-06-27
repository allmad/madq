package fs

import (
	"encoding/json"

	"github.com/chzyer/madq/go/ptrace"
)

var Stat GStat

type GStat struct {
	Flusher struct {
		BlockCopy ptrace.Size
		FlushTime ptrace.Int
		FlushSize ptrace.RatioSize
		ReadTime  ptrace.RatioTime
		WriteTime ptrace.RatioTime
	}
	Inode struct {
		Cache struct {
			NextHit   ptrace.Ratio
			InoIdxHit ptrace.Ratio
		}

		ReadDisk    ptrace.Int
		PrevSeekCnt ptrace.Ratio
	}
	File struct {
		FlushSize     ptrace.RatioSize
		FlushDuration ptrace.RatioTime
		RegenBuffer   ptrace.Ratio
	}
	Cobuffer struct {
		Trytime            ptrace.Ratio
		NotifyFlushByWrite ptrace.Ratio
		GetDataLock        ptrace.RatioTime
		GetData            ptrace.RatioTime
		FlushDelay         ptrace.RatioTime
	}
}

func (c *GStat) String() string {
	ret, _ := json.MarshalIndent(c, "", "\t")
	return string(ret)
}
