package fs

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
)

type File struct {
	ref      int32
	refGuard sync.Mutex

	flow      *flow.Flow
	cfg       *FileConfig
	ino       int32
	flags     int
	delegate  FileDelegater
	inodePool *InodePool
	flusher   FileFlusher
	cobuf     *Cobuffer

	flushWaiter sync.WaitGroup
	flushChan   chan struct{}
}

type FileDelegater interface {
	InodePoolDelegate
	ReadData(offset ShortAddr, n int) ([]byte, error)
}

type FileFlusher interface {
	WriteByInode(*InodePool, []byte, chan *FlusherWriteReply)
	Flush(wait bool)
}

type FileConfig struct {
	Ino           int32
	Name          string
	Flags         int
	Delegate      FileDelegater
	FlushInterval time.Duration
	Flusher       FileFlusher
	FlushSize     int
}

func IsFileCreate(flags int) bool {
	return flags&os.O_CREATE > 0
}

func NewFile(f *flow.Flow, cfg *FileConfig) (*File, error) {
	inodePool := NewInodePool(cfg.Ino, cfg.Delegate)

	if _, err := inodePool.GetLastest(); err != nil {
		if IsFileCreate(cfg.Flags) {
			inodePool.InitInode()
			err = nil
		} else {
			return nil, logex.Trace(err)
		}
	}

	file := &File{
		cfg:       cfg,
		flow:      f.Fork(1),
		ino:       cfg.Ino,
		flags:     cfg.Flags,
		delegate:  cfg.Delegate,
		inodePool: inodePool,
		flusher:   cfg.Flusher,
		cobuf:     NewCobuffer(1<<10, cfg.FlushSize),

		flushChan: make(chan struct{}, 1),
	}
	f.SetOnClose(func() {
		file.Close()
	})
	go file.writeLoop()

	return file, nil
}

func (f *File) Name() string {
	return f.cfg.Name
}

func (f *File) Ino() int32 {
	return f.ino
}

func (f *File) writeLoop() {
	defer f.flow.DoneAndClose()
	var (
		wantFlush bool
		wantClose bool
		timer     <-chan time.Time

		flushReply = make(chan *FlusherWriteReply, 100)
		flushStart time.Time
		buffer     []byte
		bufferOps  int
	)

	var state int
	_ = state

	for {
		select {
		case <-f.cobuf.IsWritten():
			state = 1
			timer = time.NewTimer(f.cfg.FlushInterval).C
			flushStart = time.Now()
			continue
		case <-f.cobuf.IsFlush():
			state = 2
		case <-timer:
			state = 3
			// println("file: timeout", time.Now().Sub(flushStart).String())
		case <-f.flushChan:
			state = 4
			wantFlush = true
		case reply := <-flushReply:
			state = 5
			bufferOps -= reply.N
			if reply.Err != nil {
				logex.Error("write error:", reply.Err)
			}
			continue
		case <-f.flow.IsClose():
			state = 6
			// println("want close")
			wantClose = true
		}
		timer = nil

		Stat.File.Loop.BufferDuration.AddNow(flushStart)
		n := f.cobuf.GetData(buffer)
		for n > len(buffer) {
			Stat.File.RegenBuffer.Hit()
			buffer = make([]byte, n)
			n = f.cobuf.GetData(buffer)
		}

		if n > 0 {
			Stat.File.FlushSize.AddBuf(buffer[:n])
			f.flusher.WriteByInode(f.inodePool, buffer[:n], flushReply)
			bufferOps++
		}
		if wantFlush {
			now := time.Now()
			Stat.File.Flush.WaitSize.HitN(bufferOps)
			if bufferOps > 0 {
				// println("file: flush them")
				f.flusher.Flush(false)
				// println("file: done with flush")
			}
			for bufferOps > 0 {
				reply := <-flushReply
				bufferOps -= reply.N
				if reply.Err != nil {
					logex.Error("write error:", reply.Err)
				}
			}
			Stat.File.Flush.WaitReply.AddNow(now)
			f.flushWaiter.Done()
			wantFlush = false
		}
		if wantClose {
			// println("file: remain ops:", bufferOps)
			// now := time.Now()
			if bufferOps > 0 {
				f.flusher.Flush(false)
			}
			for bufferOps > 0 {
				reply := <-flushReply
				bufferOps -= reply.N
				if reply.Err != nil {
					logex.Error("write error:", reply.Err)
				}
			}
			// println("file wait time:", time.Now().Sub(now).String())
			break
		}
	}
}

func (f *File) Stat() (*Inode, error) {
	return f.inodePool.GetLastest()
}

func (f *File) Size() int64 {
	ino, err := f.inodePool.GetLastest()
	if err != nil {
		panic(err)
	}
	return int64(ino.Start)*BlockSize + int64(ino.Size)
}

func (f *File) ReadAt(b []byte, off int64) (readBytes int, err error) {
	inode, err := f.inodePool.SeekPrev(off)
	if err != nil {
		return 0, err
	}

	var nextInode *Inode

getOffset:
	idx, ok := inode.SeekIdx(off)
	if !ok {
		nextInode, err = f.inodePool.SeekNext(inode)
		if err != nil {
			return
		}
		if inode == nextInode {
			panic("SeekNext() is not working")
		}
		inode = nextInode
		goto getOffset
	}

	remainBytes := inode.GetRemainInBlock(off)
	if remainBytes < len(b)-readBytes {
		remainBytes = len(b) - readBytes
	} else {
		remainBytes = len(b)
	}
	readAddr := inode.Offsets[idx] + ShortAddr(off&(BlockSize-1))

	readTime := time.Now()
	data, err := f.delegate.ReadData(readAddr, remainBytes)
	Stat.File.DiskRead.AddNow(readTime)
	if err != nil {
		return
	}

	if len(data) != remainBytes {
		err = io.EOF
		return
	}

	readBytes += copy(b[readBytes:], data)
	if readBytes == len(b) {
		return
	}
	off += int64(readBytes)

	goto getOffset
}

func (f *File) Write(b []byte) (int, error) {
	if f.flow.IsClosed() {
		return 0, fmt.Errorf("closed")
	}
	f.cobuf.WriteData(b)
	return len(b), nil
}

func (f *File) Sync() {
	// println("file: want sync")
	f.flushWaiter.Add(1)
	select {
	case f.flushChan <- struct{}{}:
		f.flushWaiter.Wait()
	default:
		f.flushWaiter.Done()
	}
}

func (f *File) AddRef() bool {
	succ := false
	f.refGuard.Lock()
	if f.ref >= 0 {
		f.ref++
		succ = true
	}
	f.refGuard.Unlock()
	return succ
}

func (f *File) Close() error {
	f.refGuard.Lock()
	if f.ref >= 1 {
		f.ref--
	}
	isClose := f.ref == 0
	if isClose {
		f.ref = -1
	}
	f.refGuard.Unlock()

	if !isClose {
		return nil
	}

	if !f.flow.MarkExit() {
		return nil
	}

	now := time.Now()

	f.flow.Close()
	f.cobuf.Close()
	Stat.File.CloseTime.AddNow(now)
	return nil
}
