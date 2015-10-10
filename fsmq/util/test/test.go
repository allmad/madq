package test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/logex.v1"
)

var (
	mainRoot       = ""
	ErrNotExcept   = logex.Define("result not expect")
	ErrNotEqual    = logex.Define("result not equals")
	StrNotSuchFile = "no such file or directory"
)

type testException struct {
	depth int
	info  string
}

func getMainRoot() string {
	if mainRoot != "" {
		return mainRoot
	}

	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}

	for len(cwd) > 1 {
		_, err := os.Stat(filepath.Join(cwd, ".git"))
		if err == nil {
			mainRoot = cwd + string([]rune{filepath.Separator})
			break
		}
		cwd = filepath.Dir(cwd)
	}
	return mainRoot
}

func Skip() {
	panic(nil)
}

func New(t *testing.T) {
	err := recover()
	if err == nil {
		return
	}
	te, ok := err.(*testException)
	if !ok {
		panic(err)
	}

	_, file, line, _ := runtime.Caller(5 + te.depth)
	if strings.HasPrefix(file, getMainRoot()) {
		file = file[len(getMainRoot()):]
	}
	println(fmt.Sprintf("    %s:%d: %s", file, line, te.info))
	t.FailNow()
}

func getErr(def error, e []error) error {
	if len(e) == 0 {
		return def
	}
	return e[0]
}

func Equals(o ...interface{}) {
	if len(o)%2 != 0 {
		Panic(0, "invalid Equals arguments")
	}
	for i := 0; i < len(o); i += 2 {
		equal(1, o[i], o[i+1], nil)
	}
}

func Equal(a, b interface{}, e ...error) {
	equal(1, a, b, e)
}

func CheckError(e error, s string) {
	if e == nil {
		Panic(0, ErrNotExcept)
	}
	if !strings.Contains(e.Error(), s) {
		Panic(0, s)
	}
}

func equal(d int, a, b interface{}, e []error) {
	_, oka := a.(error)
	_, okb := b.(error)
	if oka && okb {
		if !logex.Equal(a.(error), b.(error)) {
			Panic(d, fmt.Sprintf("%v: (%v, %v)",
				getErr(ErrNotEqual, e),
				a, b,
			))
		}
		return
	}
	if !reflect.DeepEqual(a, b) {
		Panic(d, fmt.Sprintf("%v: (%v, %v)", getErr(ErrNotEqual, e), a, b))
	}
}

func Should(b bool, e ...error) {
	if !b {
		Panic(0, getErr(ErrNotExcept, e))
	}
}

func NotNil(err error) {
	if err == nil {
		Panic(0, "should not nil")
	}
}

func Nil(err error) {
	if err != nil {
		Panic(0, fmt.Sprintf("should nil: %v", err))
	}
}

func Panic(depth int, obj interface{}) {
	t := &testException{
		depth: depth,
	}
	if err, ok := obj.(error); ok {
		t.info = logex.DecodeError(err)
		panic(t)
	}
	t.info = fmt.Sprint(obj)
	panic(t)
}

func GetRoot(s string) string {
	root := os.Getenv("TEST_ROOT")
	if root == "" {
		root = "/data/fsmq"
	}
	return root + s
}
