install:
	go install ./...

test: install
	go test ./...

cover:
	./goverall.sh

bench-topic:
	go test -v -benchmem -bench=. -run=Nothing github.com/chzyer/muxque/topic

bench-sync-put:
	go test -v -benchtime 5s -benchmem -bench=SyncPut -run=Nothing github.com/chzyer/muxque/bench

bench-http:
	go test -v -benchmem -bench=Http -run=Nothing github.com/chzyer/muxque/bench
