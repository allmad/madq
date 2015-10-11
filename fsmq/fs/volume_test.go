package fs

import (
	"testing"

	"github.com/chzyer/fsmq/fsmq/util/test"
)

func TestVolume(t *testing.T) {
	defer test.New(t)
	v, err := NewVolume("hello")
	test.Nil(err)
	v.Open("hello")

	_, err = NewVolume("!hello")
	test.Equal(err, ErrInvalidVolmueName)
}
