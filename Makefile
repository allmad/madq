TIME=10s
install:
	go install ./...

test: install
	go test ./...

cover:
	./goverall.sh

bench-topic:
	go test -v -benchtime $(TIME) -benchmem -bench=. -run=Nothing github.com/chzyer/muxque/topic

bench-sync-put:
	go test -v -benchtime $(TIME) -benchmem -bench=SyncPut -run=Nothing github.com/chzyer/muxque/bench

bench-http:
	go test -v -benchtime $(TIME) -benchmem -bench=Http -run=Nothing github.com/chzyer/muxque/bench
