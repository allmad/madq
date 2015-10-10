#!/bin/bash
# DO NOT EXEC DIRECTLY
go list github.com/chzyer/fsmq/fsmq 2>/dev/null 1>/dev/null
if [[ "$?" == "1" ]]; then
	mkdir -p build/src/github.com/chzyer/fsmq
	rm -fr build/src/github.com/chzyer/fsmq/fsmq
	ln -sf $(pwd)/fsmq build/src/github.com/chzyer/fsmq/fsmq
else
	rm -fr build/src
fi
