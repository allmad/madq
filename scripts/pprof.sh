#!/bin/bash
pkg=$1
shift
op=$1
shift
ops=$@

go tool pprof $ops $pkg.test $op.prof
