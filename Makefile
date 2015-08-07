TIME=10s
install:
	go install ./...

test: install
	go test -v ./...

cover:
	./scripts/goverall.sh

bench-message:
	go test -v -benchtime $(TIME) -benchmem -bench=. -run=Nothing github.com/chzyer/muxque/message | scripts/addops.awk

bench-ping:
	go test -v -benchtime $(TIME) -benchmem -bench=Ping -run=Nothing github.com/chzyer/muxque/api | scripts/addops.awk

bench-topic:
	go test -v -benchtime $(TIME) -benchmem -bench=. -run=Nothing github.com/chzyer/muxque/topic | scripts/addops.awk

bench-sync-api:
	go test -v -benchtime $(TIME) -benchmem -bench=ApiSync -run=Nothing github.com/chzyer/muxque/bench

bench-http:
	go test -v -benchtime $(TIME) -benchmem -bench=Http -run=Nothing github.com/chzyer/muxque/bench
