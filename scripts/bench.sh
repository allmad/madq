#!/bin/bash
pkg=$1
shift
ops=$@

go test -blockprofile=block.prof -cpuprofile=cpu.prof -memprofile=mem.prof $ops github.com/chzyer/madq/go/$pkg
