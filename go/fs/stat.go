package fs

import (
	"encoding/json"

	"github.com/allmad/madq/go/ptrace"
)

var Stat GStat

func ResetStat() {
	Stat = GStat{}
}

type GStat struct {
	Volume struct {
		CloseTime ptrace.RatioTime
	}
	Flusher struct {
		BlockCopy ptrace.Size
		ReadTime  ptrace.RatioTime
		HandleOp  struct {
			Total        ptrace.RatioTime
			DataArea     ptrace.RatioTime
			DataAreaCopy ptrace.RatioTime
			Partial      ptrace.RatioTime
			Inode        ptrace.RatioTime
		}
		FlushBuffer      ptrace.RatioTime
		FlushBufferGetOp ptrace.RatioTime
		Buffering        struct {
			Size ptrace.RatioSize
		}
		Flush struct {
			Total    ptrace.RatioTime
			Count    ptrace.Int
			Size     ptrace.RatioSize
			RawWrite ptrace.RatioTime
		}
		CloseTime        ptrace.RatioTime
		FlushBufferAddOp ptrace.RatioTime
		DataSlice        struct {
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
		FlushSize   ptrace.RatioSize
		RegenBuffer ptrace.Ratio
		CloseTime   ptrace.RatioTime
		Loop        struct {
			BufferDuration ptrace.RatioTime
		}
		Flush struct {
			WaitSize  ptrace.Ratio
			WaitReply ptrace.RatioTime
		}
	}
	Cobuffer struct {
		Trytime            ptrace.Ratio
		NotifyFlushByWrite ptrace.Ratio
		GetData            struct {
			Lock ptrace.RatioTime
			Copy ptrace.RatioTime
			Size ptrace.RatioSize
		}
		Grow       ptrace.RatioTime
		FlushDelay ptrace.RatioTime
		FullTime   ptrace.RatioTime
		WriteTime  ptrace.RatioTime
	}
}

func (c *GStat) String() string {
	ret, _ := json.MarshalIndent(c, "", "\t")
	return string(ret)
}
