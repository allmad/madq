TIME=10s
install:
	go install ./...

test: install
	go test ./...

cover:
	./scripts/goverall.sh

find-todo:
	@find . -name '*.go' | xargs grep -n TODO

bench-message:
	go test -v -benchtime $(TIME) -benchmem -bench=. -run=Nothing github.com/chzyer/muxque/rpc/message | scripts/addops.awk

bench-ping:
	go test -v -benchtime $(TIME) -benchmem -bench=Ping -run=Nothing github.com/chzyer/muxque/api | scripts/addops.awk

bench-topic:
	go test -v -benchtime $(TIME) -benchmem -bench=. -run=Nothing github.com/chzyer/muxque/muxque/topic | scripts/addops.awk

bench-sync-api:
	go test -v -benchtime $(TIME) -benchmem -bench=ApiSync -run=Nothing github.com/chzyer/muxque/debug/bench | scripts/addops.awk

bench-http:
	go test -v -benchtime $(TIME) -benchmem -bench=Http -run=Nothing github.com/chzyer/muxque/debug/bench | scripts/addops.awk

bench-file:
	go test -v -benchtime 5s -benchmem -bench=Write256 -run=Nothing github.com/chzyer/muxque/utils/bitmap | scripts/addops.awk
