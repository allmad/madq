package common

import (
	"fmt"
	"os"
	"syscall"
)

const (
	LOCK_SH = 1
	LOCK_EX = 2
	LOCK_NB = 4 // non-blocking
	LOCK_UN = 8
)

type Flock struct {
	fd *os.File
}

func NewFlock(dir string) (*Flock, error) {
	fd, err := os.OpenFile(dir, 0, 0755)
	if err != nil {
		return nil, err
	}

	if err := syscall.Flock(int(fd.Fd()), LOCK_EX|LOCK_NB); err != nil {
		return nil, fmt.Errorf("error in locking dir %v: %v", dir, err)
	}

	return &Flock{fd}, nil
}

func (f *Flock) Unlock() error {
	err := syscall.Flock(int(f.fd.Fd()), LOCK_UN|LOCK_NB)
	f.fd.Close()
	return err
}

func LockDir(dir string) (*Flock, error) {
	if stat, _ := os.Stat(dir); stat != nil {
		flock, err := NewFlock(dir)
		if err != nil {
			return nil, err
		}
		if err := flock.Unlock(); err != nil {
			return nil, err
		}

		if err := os.RemoveAll(dir); err != nil {
			return nil, err
		}
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	return NewFlock(dir)
}
