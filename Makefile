build_list=$(shell go list ./... | grep -v muxque)
export GOPATH=$(shell echo $$GOPATH):$(shell pwd)/build:$(shell pwd)/deps
export GOBIN=$(shell pwd)/build/bin
.PHONY: deps

build/bin/fsmq: deps
	@mkdir -p build
	@scripts/check_source.sh
	@make -C fsmq

deps:
	@git submodule init
	@git submodule sync >/dev/null
	@git submodule update

test:
	@make -C fsmq test

clean:
	go clean ./...
	rm -fr build
	git submodule deinit .

cover:
	@make -C fsmq cover

find-todo:
	@find . -name '*.go' | xargs grep -n TODO
