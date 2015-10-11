package fs

import (
	"regexp"

	"gopkg.in/logex.v1"
)

var (
	regexpVolumeName = regexp.MustCompile(`^[\w\d\_]+$`)

	ErrInvalidVolmueName = logex.Define("fs: invalid volume name")
)

type Volume struct {
	Name string
}

func NewVolume(name string) (*Volume, error) {
	if !regexpVolumeName.MatchString(name) {
		return nil, ErrInvalidVolmueName
	}
	vol := &Volume{
		Name: name,
	}
	return vol, nil
}

func (v *Volume) Open(name string) {
	return
}
