package fs

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
)

type fileWriteOp struct {
	b     []byte
	reply chan error
}

type File struct {
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
	writeChan   chan *fileWriteOp
	replyPool   sync.Pool
}

type FileDelegater interface {
	InodePoolDelegate
	ReadData(offset ShortAddr, n int) ([]byte, error)
}

type FileFlusher interface {
	WriteByInode(*InodePool, []byte, chan error)
	Flush()
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
		cobuf:     NewCobuffer(cfg.FlushSize, cfg.FlushSize),

		flushChan: make(chan struct{}, 1),
		writeChan: make(chan *fileWriteOp, 8),
		replyPool: sync.Pool{
			New: func() interface{} {
				return make(chan error)
			},
		},
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
		timer     <-chan time.Time

		flushReply = make(chan error, 100)
	)

loop:
	for {
		select {
		case <-f.cobuf.IsWritten():
			timer = time.NewTimer(f.cfg.FlushInterval).C
		case <-f.cobuf.IsFlush():
			goto flush
		case <-timer:
			goto flush
		case <-f.flushChan:
			wantFlush = true
			f.cobuf.Flush()
		case err := <-flushReply:
			if err != nil {
				logex.Error("write error:", err)
			}
		case <-f.flow.IsClose():
			break loop
		}
		continue

	flush:
		buffer := f.cobuf.GetData()
		f.flusher.WriteByInode(f.inodePool, buffer, flushReply)
		if wantFlush {
			f.flushWaiter.Done()
			wantFlush = false
		}
	}
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
	}
	readAddr := inode.Offsets[idx] + ShortAddr(off&(BlockSize-1))
	data, err := f.delegate.ReadData(readAddr, remainBytes)
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

func (f *File) WriteData(b []byte, reply chan error) {
	f.writeChan <- &fileWriteOp{
		b:     b,
		reply: reply,
	}
}

func (f *File) Write(b []byte) (int, error) {
	f.cobuf.WriteData(b)
	return len(b), nil
}

func (f *File) Sync() {
	f.flushWaiter.Add(1)
	select {
	case f.flushChan <- struct{}{}:
		f.flushWaiter.Wait()
	default:
		f.flushWaiter.Done()
	}

	f.flusher.Flush()
}

func (f *File) Close() error {
	if !f.flow.MarkExit() {
		return nil
	}

	f.flow.Close()
	f.cobuf.Close()
	return nil
}

type FileReader struct {
	*File
	offset int64
}

func NewFileReader(f *File, off int64) *FileReader {
	return &FileReader{
		File:   f,
		offset: off,
	}
}

func (f *FileReader) Read(b []byte) (int, error) {
	n, err := f.File.ReadAt(b, f.offset)
	f.offset += int64(n)
	return n, err
}
