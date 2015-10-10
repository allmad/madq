export GOPATH=$(shell echo $$GOPATH):$(shell pwd)/build:$(shell pwd)/deps
export GOBIN=$(shell pwd)/build/bin
export PKG=github.com/chzyer/fsmq
.PHONY: deps

build/bin/fsmq: bootstrap
	@make -C fsmq

bootstrap: deps
	@mkdir -p build
	@env lib=$(PKG) scripts/check_source.sh

deps:
	@git submodule init
	@git submodule sync >/dev/null
	@git submodule update

test: bootstrap
	go test -v $(PKG)/fsmq/...

clean:
	go clean ./...
	rm -fr build
	git submodule deinit .

cover: bootstrap
	@make -C fsmq cover

show-cover:
	@make -C fsmq show-cover

find-todo:
	@find . -name '*.go' | xargs grep -n TODO
