# Muxque (WIP)

[![Build Status](https://travis-ci.org/chzyer/muxque.svg?branch=master)](https://travis-ci.org/chzyer/muxque)
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg)](LICENSE.md)

muxque is design to use in IM, follows pub/sub model, to achieve serve millions users by multiplex sockets.

### goal
* possible to subscribe multiple topics/channels in one client.
* always persist messages to disk to keep them safe
* zero-extra-cost(disk) for multicasted messages/channels
* distributed and decentralized topologies without single points of failure
* the number of topic can be unlimit(depends on hardware)
* provide a horizontally scaling solution

### benchmark

* http test
```
$ go run bench/httpserver/server.go
$ make bench-http
go test -v -benchtime 10s -benchmem -bench=Http -run=Nothing github.com/chzyer/muxque/bench
testing: warning: no tests to run
PASS
BenchmarkHttpPut	 3000000	      4808 ns/op (aka 207,986 rps)	      16 B/op	       0 allocs/op
ok  	github.com/chzyer/muxque/bench	18.811s
```

* sync api (single client wait until server reply)
```
$ go run muxque.go
$ make bench-sync-api
go test -v -benchtime 10s -benchmem -bench=ApiSync -run=Nothing github.com/chzyer/muxque/bench
testing: warning: no tests to run
PASS
BenchmarkApiSyncGet	 3000000	      4540 ns/op (aka 220,264 rps)	     332 B/op	       3 allocs/op
BenchmarkApiSyncPut	 5000000	      3355 ns/op (aka 298,062 rps)	       2 B/op	       0 allocs/op
ok  	github.com/chzyer/muxque/bench	52.065s
```

* internal test (without network)
```
$ make bench-topic
go test -v -benchtime 10s -benchmem -bench=. -run=Nothing github.com/chzyer/muxque/topic
PASS
BenchmarkTopicGet	 5000000	      2805 ns/op (381,388 rps)	     432 B/op	       8 allocs/op
BenchmarkTopicPut	 5000000	      2622 ns/op (356,506 rps)	     119 B/op	       3 allocs/op
ok  	github.com/chzyer/muxque/topic	46.333s
```

nsq
```
# using --mem-queue-size=1000000 --data-path= --size=200 --batch-size=200
# compiling/running nsqd
# creating topic/channel
# compiling bench_reader/bench_writer
PUB: [bench_writer] 2015/07/19 20:07:15 duration: 10.003577746s - 29.725mb/s - 155844.243ops/s - 6.417us/op
SUB: [bench_reader] 2015/07/19 20:07:25 duration: 10.014623761s - 17.258mb/s - 90481.282ops/s - 11.052us/op
```
