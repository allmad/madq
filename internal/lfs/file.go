package lfs

import (
	"fmt"
	"io"

	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/bio"
)

var (
	ErrFileClosed = logex.Define("file is closed")
)

type FileDelegate interface {
	GetFlushNotify() chan struct{}
	MallocWriter(n int) *bio.DeviceWriter
	NextInode(*Inode) (*Inode, error)
	PrevInode(*Inode) (*Inode, error)
	ForceFlush()
	ReadDeviceAt([]byte, int64) (int, error)
	GetInode(ino int32) (*Inode, error)
	DoneFlush()
}

type File struct {
	flow     *flow.Flow
	name     string
	ino      int32
	inode    *Inode
	delegate FileDelegate
	offset   int64

	wchan chan *writeReq
}

func NewFile(f *flow.Flow, delegate FileDelegate, ino int32, name string) (*File, error) {
	fd := &File{
		delegate: delegate,
		ino:      ino,
		name:     name,
		wchan:    make(chan *writeReq, 8),
	}
	inode, err := delegate.GetInode(ino)
	if err != nil {
		return nil, logex.Trace(err, fmt.Sprint("ino:", ino))
	}
	fd.inode = inode

	go fd.loop()
	f.ForkTo(&fd.flow, fd.Close)
	return fd, nil
}

func (f *File) loop() {
	f.flow.Add(1)
	defer f.flow.DoneAndClose()

	inode := f.inode
	currentIdx := -1

	var wantFlush chan struct{}
	var memWriter *bio.DeviceWriter
	getNewInode := func() {
		panic("new to make a new one")
	}
loop:
	for {
		select {
		case wreq := <-f.wchan:
			if memWriter == nil {
				wantFlush = f.delegate.GetFlushNotify()
				memWriter = f.delegate.MallocWriter(BlockSize)
				currentIdx = inode.FindAvailable()
				if currentIdx == -1 {
					getNewInode()
				}
			}

			data := wreq.Data
		write:
			n := memWriter.Byte(data)
			if inode.BlockMeta[currentIdx].IsEmpty() {
				inode.SetBlock(currentIdx, n, memWriter.Offset())
			} else {
				inode.AddBlockSize(currentIdx, n)
			}

			if len(data[n:]) > 0 {
				// we got more to write
				data = data[n:]
				newMemWriter := f.delegate.MallocWriter(BlockSize)
				memWriter = newMemWriter
				currentIdx = inode.FindAvailable()
				if currentIdx == -1 {
					getNewInode()
				}
				goto write
			}

			// finish
			wreq.Reply <- &writeResp{len(wreq.Data), nil}
			if memWriter.Available() == 0 {
				memWriter = nil
				wantFlush = nil
			}
		case <-wantFlush:
			wantFlush = nil
			memWriter = nil
			f.delegate.DoneFlush()
			// flush inode to buffer
		case <-f.flow.IsClose():
			break loop
		}
	}
}

func (f *File) Read(b []byte) (n int, err error) {
	n, err = f.ReadAt(b, f.offset)
	f.offset += int64(n)
	return n, err
}

func (f *File) ReadAt(b []byte, offset int64) (n int, err error) {
	// now need to translate to raw offset
	if offset > f.inode.Start {

	}
	return 0, io.EOF

	return f.delegate.ReadDeviceAt(b, offset)
}

func (f *File) Write(b []byte) (n int, err error) {
	ret := make(chan *writeResp)
	f.wchan <- &writeReq{
		Data:  b,
		Reply: ret,
	}
	resp := <-ret
	return resp.N, resp.Err
}

func (f *File) Flush() {
	f.delegate.ForceFlush()
}

func (f *File) Name() string {
	return f.name
}

func (f *File) Close() {
	f.flow.Close()
	for wreq := range f.wchan {
		wreq.Reply <- &writeResp{
			N:   0,
			Err: ErrFileClosed.Trace(),
		}
	}
	// read all wchan
}

// -----------------------------------------------------------------------------

type writeReq struct {
	Data  []byte
	Reply chan *writeResp
}

type writeResp struct {
	N   int
	Err error
}
