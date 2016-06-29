// +build go1.5

package bench

import (
	"io/ioutil"
	"runtime/trace"
)

func EnableTrace() {
	ff, err := ioutil.TempFile("", "")
	if err != nil {
		println(err.Error())
		return
	}
	defer ff.Close()
	println("trace file:", ff.Name())
	trace.Start(ff)
}

func DisableTrace() {
	trace.Stop()
}
