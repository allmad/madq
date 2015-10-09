build_list=$(shell go list ./... | grep -v muxque)
export GOPATH=$(shell pwd)/build:$(shell pwd)/deps
export GOBIN=$(shell pwd)/build/bin
.PHONY: deps

build/bin/fsmq: deps
	@mkdir -p build
	@make -C fsmq

deps:
	@make -C deps

clean:
	rm -fr build

cover:
	@./scripts/goverall.sh

find-todo:
	@find . -name '*.go' | xargs grep -n TODO
