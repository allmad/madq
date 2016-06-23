package fs

import "fmt"

type FileName [24]byte

type Volume struct {
}

func NewVolume() *Volume {
	return &Volume{}
}

func (v *Volume) Open(name string) (*File, error) {
	if len(name) > 24 {
		return nil, fmt.Errorf("filename exceed 24bytes")
	}
	return nil, nil
}

func (v *Volume) Close() {

}
