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
	flow          *flow.Flow
	ino           int32
	flags         int
	delegate      FileDelegater
	inodePool     *InodePool
	flushInterval time.Duration
	flusher       FileFlusher

	flushWaiter sync.WaitGroup
	flushChan   chan struct{}
	writeChan   chan *fileWriteOp
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
	Flags         int
	Delegate      FileDelegater
	FlushInterval time.Duration
	FileFlusher   FileFlusher
}

func NewFile(f *flow.Flow, cfg *FileConfig) (*File, error) {
	inodePool := NewInodePool(cfg.Ino, cfg.Delegate)

	if _, err := inodePool.GetLastest(); err != nil {
		if cfg.Flags&os.O_CREATE > 0 {
			inodePool.InitInode()
			err = nil
		} else {
			return nil, err
		}
	}

	file := &File{
		flow:          f.Fork(1),
		ino:           cfg.Ino,
		flags:         cfg.Flags,
		delegate:      cfg.Delegate,
		flushInterval: cfg.FlushInterval,
		inodePool:     inodePool,
		flusher:       cfg.FileFlusher,

		flushChan: make(chan struct{}, 1),
		writeChan: make(chan *fileWriteOp, 8),
	}
	f.SetOnClose(func() {
		file.Close()
	})
	go file.writeLoop()

	return file, nil
}

func (f *File) writeLoop() {
	defer f.flow.DoneAndClose()
	var (
		timer = time.NewTimer(time.Second)
	)
	timer.Stop()

	var buffer []byte
	var flushReply = make(chan error, 1)
	var wantFlush bool

loop:
	for {
		select {
		case op := <-f.writeChan:
			buffer = append(buffer, op.b...)
			op.reply <- nil
			timer.Reset(f.flushInterval)

			if !wantFlush {
			buffering:
				for {
					select {
					case <-timer.C:
						break buffering
					case <-f.flushChan:
						wantFlush = true
						break buffering
					case <-f.flow.IsClose():
						break buffering
					case op := <-f.writeChan:
						buffer = append(buffer, op.b...)
						op.reply <- nil
					}
				}
			}

			f.flusher.WriteByInode(f.inodePool, buffer, flushReply)
			if wantFlush {
				f.flushWaiter.Done()
				wantFlush = false
			}
			buffer = buffer[:0]
		case <-f.flushChan:
			if len(f.writeChan) != 0 {
				wantFlush = true
			} else {
				f.flushWaiter.Done()
			}
		case err := <-flushReply:
			// send to Write() ?
			if err != nil {
				logex.Error("write error:", err)
			}
		case <-f.flow.IsClose():
			break loop
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
		inode = nextInode
		goto getOffset
	}

	remainBytes := inode.GetRemainInBlock(off)
	data, err := f.delegate.ReadData(inode.Offsets[idx], remainBytes)
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
	op := &fileWriteOp{
		b:     b,
		reply: make(chan error),
	}
	f.writeChan <- op
	err := <-op.reply
	if err != nil {
		return 0, err
	}
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
	f.flow.Close()
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
