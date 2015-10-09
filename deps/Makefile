export GOPATH := $(shell pwd)
export GOBIN := $(shell pwd)/bin
export pkg := $(shell cat pkg.list | awk '{print $$1}')
export pkgsrc := $(shell cat pkg.list | sed 's/ /\//g' | sed 's/^/vers\//g')
.PHONY: mark

check: $(pkgsrc)
	@go install $(pkg)

all: mark check

mark:
	@./mark

clean:
	rm -fr pkg/ src/ bin/

vers/%:
	go get -u -v $(shell bash -c "dirname `echo "$@"|sed 's/vers\///'`")
	@mkdir -p $(shell dirname $@)
	@rm -f $(shell dirname $@)/*
	@touch $@
