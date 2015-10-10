#!/bin/bash
# DO NOT EXEC DIRECTLY
go list github.com/chzyer/fsmq/fsmq 2>/dev/null 1>/dev/null
fail="$?"
rm -fr build/src
if [[ "$fail" == "1" ]]; then
	mkdir -p build/src/github.com/chzyer/fsmq
	ln -sf $(pwd)/fsmq build/src/github.com/chzyer/fsmq/fsmq
fi
