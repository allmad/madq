package fs

import (
	"encoding/json"

	"github.com/chzyer/madq/go/ptrace"
)

var Stat GStat

type GStat struct {
	Volume struct {
		CloseTime ptrace.RatioTime
	}
	Flusher struct {
		BlockCopy         ptrace.Size
		FlushTime         ptrace.Int
		FlushSize         ptrace.RatioSize
		ReadTime          ptrace.RatioTime
		WriteTime         ptrace.RatioTime
		HandleOp          ptrace.RatioTime
		HandleOpData      ptrace.RatioTime
		HandleOpDataWrite ptrace.RatioTime
		HandleOpPartial   ptrace.RatioTime
		HandleOpInode     ptrace.RatioTime
		FlushLoop         ptrace.RatioTime
		FlushBuffer       ptrace.RatioTime
		FlushBufferGetOp  ptrace.RatioTime
		Flush             ptrace.RatioTime
		CloseTime         ptrace.RatioTime
		CloseFlush        ptrace.RatioTime
		FlushBufferAddOp  ptrace.RatioTime
		DataSlice         struct {
		}
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
		CloseTime     ptrace.RatioTime
	}
	Cobuffer struct {
		Trytime            ptrace.Ratio
		NotifyFlushByWrite ptrace.Ratio
		GetDataLock        ptrace.RatioTime
		GetData            ptrace.RatioTime
		FlushDelay         ptrace.RatioTime
		FullTime           ptrace.RatioTime
		WriteTime          ptrace.RatioTime
	}
}

func (c *GStat) String() string {
	ret, _ := json.MarshalIndent(c, "", "\t")
	return string(ret)
}
