export GOPATH=$(shell echo $$GOPATH):$(shell pwd)/deps
export GOBIN=$(shell pwd)/bin
export PKG=github.com/chzyer/fsmq
.PHONY: deps

bin/fsmq: deps
	@make -C fsmq

deps:
	@git submodule init
	@git submodule sync >/dev/null
	@git submodule update

test: deps
	@make -C fsmq test

clean:
	go clean ./...
	rm -fr bin
	git submodule deinit .

cover: deps
	@make -C fsmq cover

show-cover:
	@make -C fsmq show-cover

find-todo:
	@find . -name '*.go' | xargs grep -n TODO
