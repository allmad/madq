package utils

import (
	"testing"

	"gopkg.in/logex.v1"
)

func TestGetRoot(t *testing.T) {
	var err error
	defer TDefer(t, &err)

	if GetRoot("/hello") != "/data/muxque/hello" {
		err = logex.NewError("result not expected")
		return
	}
}

func TestState(t *testing.T) {
	var err error
	defer TDefer(t, &err)

	var s State
	if s.IsClosed() {
		err = logex.NewError("state not expected closed")
		return
	}
	if !s.ToClose() {
		err = logex.NewError("expect can close")
		return
	}
	if s.ToClose() {
		err = logex.NewError("expect can't close")
		return
	}

	return
}
