#!/bin/bash

if [[ ! -d '.git' ]]; then
	echo "please call in main directory" >&2
	exit 1
fi

if [[ "$lib" == "" ]]; then
	echo 'Please specify $lib' >&2
	exit 1
fi

go list $lib 2>/dev/null 1>/dev/null

name=fsmq
fail="$?"
rm -fr build/src
if [[ "$fail" == "1" ]]; then
	mkdir -p build/src/$lib
	ln -sf $(pwd)/$name build/src/$lib/$name
fi
