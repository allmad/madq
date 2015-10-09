package lfs

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chzyer/fsmq/rpc"
	"github.com/chzyer/fsmq/utils"
	"github.com/chzyer/fsmq/utils/bitmap"

	"gopkg.in/logex.v1"
)

const (
	BlkBit     = 12
	SegmentBit = 22

	MaxWriteBuf    = 4 << 20
	FlushThreshold = int(MaxWriteBuf*80) / 100
	FlushInterval  = 10 * time.Millisecond
)

var (
	ErrShortWrite = logex.Define("write too short")
)

// log structured file system implementation
// provide sequence-write/random-read on large topics
type Ins struct {
	cfg     *Config
	cp      *checkPoint
	cpGuard sync.Mutex

	rfd *bitmap.File
	wfd *bitmap.File

	wch chan *wctx

	stopCh chan struct{}

	ofs      map[string]*File
	ofsGuard sync.RWMutex
}

type Config struct {
	BasePath   string
	BlkBit     uint
	SegmentBit int

	blkSize  int
	emptyBlk []byte
}

func (c *Config) init() error {
	if c.BlkBit == 0 {
		c.BlkBit = BlkBit
	}
	if c.SegmentBit == 0 {
		c.SegmentBit = SegmentBit
	}
	c.blkSize = 1 << c.BlkBit
	c.emptyBlk = make([]byte, c.blkSize)
	return nil
}

// TODO: lock the directory
func New(cfg *Config) (*Ins, error) {
	if err := cfg.init(); err != nil {
		return nil, logex.Trace(err)
	}
	rfd, err := bitmap.NewFile(cfg.BasePath)
	if err != nil {
		return nil, logex.Trace(err)
	}
	wfd, err := bitmap.NewFile(cfg.BasePath)
	if err != nil {
		return nil, logex.Trace(err)
	}

	ins := &Ins{
		cfg:    cfg,
		rfd:    rfd,
		wfd:    wfd,
		wch:    make(chan *wctx, 1<<3),
		stopCh: make(chan struct{}),
		cp:     newCheckPoint(cfg.BlkBit, rfd),
		ofs:    make(map[string]*File),
	}
	go ins.readloop()
	go ins.writeloop()
	return ins, nil
}

func (i *Ins) readloop() {

}

type wctx struct {
	f     *File
	off   int64
	buf   []byte
	reply chan error
}

func (i *Ins) writeloop() {
	var (
		ctx   *wctx
		ctxs  []*wctx
		flush bool
		err   error

		blkbuf   = utils.NewBlk(MaxWriteBuf)
		timer    = time.NewTimer(FlushInterval)
		blkSize  = i.cfg.blkSize
		rawStart = i.cp.blkOff
	)

	for {
		timer.Reset(FlushInterval)
		if ctx != nil {
			select {
			case ctx = <-i.wch:
				ctxs = append(ctxs, ctx)
			case <-timer.C:
				flush = true
			case <-i.stopCh:
				return
			}
		}

		if ctx != nil {
			if blkbuf.Remain() < len(ctx.buf)+blkSize { // inode: blkSize
				if len(ctx.buf) > MaxWriteBuf {
					// just write
					continue
				}
				goto flush
			}

			blkStart := rawStart + int64(blkbuf.Len())
			ctx.f.ino.ExtBlks(blkStart, len(ctx.buf))
			_, err = blkbuf.Write(ctx.buf)
			if err != nil {
				ctx.reply <- logex.Trace(err)
				continue
			}

			// i.cp.SetInoOffset(ctx.f.name, woff+int64(inoOff))
			// reply
			ctx.reply <- nil
			ctx = nil
		}

		if blkbuf.Len() > FlushThreshold {
			flush = true
		}
		if !flush || blkbuf.Len() == 0 {
			continue
		}

	flush:

		rawOff, _ := i.allocBlks(blkbuf.Bytes())
		n, err := i.wfd.WriteAt(blkbuf.Bytes(), rawOff)
		if err == nil && n != blkbuf.Len() {
			err = ErrShortWrite
		}
		if err != nil {
			// notify all
			logex.Error(err)
		}
		//blkbuf.WriteTo(i.wfd)
		ctxs = ctxs[:0]
		flush = false
		rawStart = i.cp.blkOff
	}
}

func (i *Ins) OpenReader(name string) (*utils.Reader, error) {
	f, err := i.Open(name)
	if err != nil {
		return nil, err
	}
	return &utils.Reader{f, 0}, nil
}

func (i *Ins) OpenWriter(name string) (*utils.Writer, error) {
	f, err := i.Open(name)
	if err != nil {
		return nil, err
	}
	return &utils.Writer{f, f.Size()}, nil
}

// make a new one if not found
// TODO: there is two locks here
func (i *Ins) findIno(name string, blkBit uint) *Inode {
	ino, _ := NewInode(rpc.NewString(name), blkBit)
	off := i.cp.GetInoOffset(name)
	if off > 0 {
		err := ino.PRead(utils.NewBufio(utils.NewReader(i.rfd, off)))
		if err != nil {
			logex.Error(err)
		}
	}
	return ino
}

func (i *Ins) Open(name string) (*File, error) {
	i.ofsGuard.RLock()
	f := i.ofs[name]
	i.ofsGuard.RUnlock()
	if f != nil {
		return f, nil
	}

	i.ofsGuard.Lock()
	f, err := openFile(i, i.findIno(name, i.cfg.BlkBit), name)
	if err == nil {
		i.ofs[name] = f
	}
	i.ofsGuard.Unlock()
	return f, logex.Trace(err)
}

func (i *Ins) calBlkcnt(size int) int {
	blkcnt := size >> i.cfg.BlkBit
	if size&(i.cfg.blkSize-1) != 0 {
		blkcnt++
	}
	return blkcnt
}

// return offsets
func (i *Ins) allocBlks(p []byte) (startOff int64, cnt int) {
	blkcnt := i.calBlkcnt(len(p))
	startOff = i.cp.blkOff
	i.cp.blkOff += int64(blkcnt) * int64(i.cfg.blkSize)
	return startOff, blkcnt
}

func (i *Ins) closeFile(f *File) error {
	i.ofsGuard.Lock()
	delete(i.ofs, f.name)
	i.ofsGuard.Unlock()
	return nil
}

// 1. len(p) <= blk
// 2. 2*blk > len(p) > blk, continuous
// 3. 2*blk > len(p) > blk, not continuous
func (o *Ins) readAt(f *File, p []byte, off int64) (int, error) {
	blkIdx, rawOff := f.ino.GetRawOff(off)
	if blkIdx < 0 {
		return 0, io.EOF
	}
	blkOff, blkSize := f.ino.GetBlk(blkIdx)
	pOff := 0
	pEnd := 0
	bytesRead := 0

	// println()
	// try to find the large continuous blocks
	for bytesRead < len(p) {
		pEnd += blkSize
		if pEnd == blkSize { // first time, maybe some offset exists
			pEnd -= int(rawOff - blkOff)
		}
		if pEnd >= len(p) {
			pEnd = len(p) // last one
		} else {
			lastBlkEnd := blkOff + int64(blkSize)
			blkIdx++
			if f.ino.HasBlk(blkIdx) {
				blkOff, blkSize = f.ino.GetBlk(blkIdx)
				if blkOff == lastBlkEnd { // continuous
					continue
				}
			} else {
				blkOff, blkSize = -1, -1
			}
		}

		// println("readat:", pOff, pEnd, rawOff, pEnd-pOff, len(p), off)
		n, err := o.rfd.ReadAt(p[pOff:pEnd], rawOff)
		bytesRead += n
		if err != nil {
			return bytesRead, logex.Trace(err)
		}
		if blkOff == -1 {
			return bytesRead, logex.Trace(io.EOF)
		}

		rawOff = blkOff
		pOff = pEnd
	}
	return bytesRead, nil
}

func (o *Ins) fillBuf(p []byte, extendSize int) ([]byte, int, int) {
	fillsize := 0
	remain := len(p) & (o.cfg.blkSize - 1)

	if remain > 0 {
		fillsize += o.cfg.blkSize - remain
		p = append(p, o.cfg.emptyBlk[:o.cfg.blkSize-remain]...)
	}

	for i := 0; i < extendSize; i++ {
		fillsize += o.cfg.blkSize
		p = append(p, o.cfg.emptyBlk...)
	}

	return p, fillsize, remain
}

func (o *Ins) calInoRawSize(newData []byte, ino *Inode) int {
	return ino.RawSize(o.calBlkcnt(len(newData)))
}

func (o *Ins) plusBlkOffset(start int64, size int) int64 {
	return start + int64(size)<<o.cfg.BlkBit
}

var (
	a, b, c int64
)

func init() {
	go func() {
		for _ = range time.Tick(time.Second) {
			a := atomic.SwapInt64(&a, 0)
			b := atomic.SwapInt64(&b, 0)
			c := atomic.SwapInt64(&c, 0)
			if b != 0 {
				println(time.Duration(a/b).String(), c/b, b)
			}
		}
	}()
}

func (o *Ins) writeAt(f *File, p []byte, off int64) (n int, err error) {
	ctx := &wctx{
		f:     f,
		off:   off,
		buf:   p,
		reply: make(chan error),
	}
	select {
	case o.wch <- ctx:
		err = <-ctx.reply
	case <-o.stopCh:
		err = errors.New("closed")
	}

	return len(p), err
}

func (i *Ins) Pruge() {
	i.Close()
	i.wfd.Delete()
}

func (i *Ins) FlushCheckPoint() error {
	return logex.Trace(i.cp.Save(&utils.Writer{i.wfd, i.wfd.Size()}))
}

func (i *Ins) Close() {
	close(i.stopCh)
	i.rfd.Close()
	if err := i.FlushCheckPoint(); err != nil {
		logex.Error(err)
	}
	i.wfd.Close()
}
