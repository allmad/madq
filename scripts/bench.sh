#!/bin/bash
pkg=$1
shift
ops=$@

go test -blockprofile=${pkg}_block.prof -cpuprofile=${pkg}_cpu.prof -memprofile=${pkg}_mem.prof $ops github.com/chzyer/madq/go/$pkg
