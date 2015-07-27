install:
	go install ./...

test: install
	go test ./...

cover:
	./goverall.sh

bench-topic:
	go test -v -benchmem -bench=. -run=Nothing github.com/chzyer/muxque/topic
