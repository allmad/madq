TIME=10s
install:
	go install ./...

test: install
	go test -v ./...

cover:
	./goverall.sh

bench-topic:
	go test -v -benchtime $(TIME) -benchmem -bench=. -run=Nothing github.com/chzyer/muxque/topic

bench-sync-api:
	go test -v -benchtime $(TIME) -benchmem -bench=ApiSync -run=Nothing github.com/chzyer/muxque/bench

bench-http:
	go test -v -benchtime $(TIME) -benchmem -bench=Http -run=Nothing github.com/chzyer/muxque/bench
