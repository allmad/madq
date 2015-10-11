# [WIP] FsMQ

[![Build Status](https://travis-ci.org/chzyer/fsmq.svg?branch=master)](https://travis-ci.org/chzyer/fsmq)
[![Coverage Status](https://coveralls.io/repos/chzyer/fsmq/badge.svg?branch=master&service=github)](https://coveralls.io/github/chzyer/fsmq?branch=master)
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg)](LICENSE.md)

FsMQ stand for FileSystem as Message Queue

## Build from source

* [Golang](https://golang.org/dl/) >= 1.4.1

```{shell}
$ go get github.com/chzyer/fsmq
$ cd $GOPATH/src/github.com/chzyer/fsmq
$ make # everything will done
$ bin/fsmq -h # execute fsmq now!
```

Notice:  
If you meet `cannot download, $GOPATH not set.`  
please set your `$GOPATH` or just execute `export GOPATH=$(pwd)` in shell.
