package fs

import (
	"os"
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

	flushChan chan struct{}
	writeChan chan *fileWriteOp
}

type FileDelegater interface {
	InodePoolDelegate
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
	var flushReply = make(chan error)

loop:
	for {
		select {
		case op := <-f.writeChan:
			buffer = append(buffer, op.b...)
			op.reply <- nil

			timer.Reset(f.flushInterval)
		buffering:
			for {
				select {
				case <-timer.C:
					break buffering
				case <-f.flushChan:
					break buffering
				case op := <-f.writeChan:
					buffer = append(buffer, op.b...)
					op.reply <- nil
				}
			}

			f.flusher.WriteByInode(f.inodePool, buffer, flushReply)
		case err := <-flushReply:
			// send to Write() ?
			logex.Error("write error:", err)
		case <-f.flow.IsClose():
			break loop
		}
	}
}

func (f *File) ReadAt(b []byte, off int64) (int, error) {
	return 0, nil
}

func (f *File) Write(b []byte) (int, error) {
	op := &fileWriteOp{
		b: b, reply: make(chan error),
	}
	f.writeChan <- op
	err := <-op.reply
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (f *File) Flush() {
	select {
	case f.flushChan <- struct{}{}:
	default:
	}
	f.flusher.Flush()
}

func (f *File) Close() error {
	f.flow.Close()
	return nil
}
