# FsMQ (WIP)

[![Build Status](https://travis-ci.org/chzyer/fsmq.svg?branch=master)](https://travis-ci.org/chzyer/fsmq)
[![Coverage Status](https://coveralls.io/repos/chzyer/fsmq/badge.svg?branch=master&service=github)](https://coveralls.io/github/chzyer/fsmq?branch=master)
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg)](LICENSE.md)

FsMQ stand for FileSystem as Message Queue

### goal
---

* possible to subscribe multiple topics/channels in one client.
* always persist messages to disk to keep them safe
* zero-extra-cost(disk) for multicasted messages/channels
* distributed and decentralized topologies without single points of failure
* the number of topic can be unlimit(depends on hardware)
* provide a horizontally scaling solution
* using different way to support large number of topic (configurable)


### roadmap
---

* [x] svr: topic-basic get/put
* [x] sdk: has a simple version (/api)
* [x] sdk: add a simple consumer
* [ ] sdk: add a sync producer
* [ ] sdk: consumer support subscribe multiple topics
* [ ] sdk: consumer save offset to disk
* [ ] sdk/svr: producer support publish messages of different topics multiplex in one batch call
* [ ] svr: add a partition auto-scale solution
* [ ] debugTools: detect error in persistence msgs
* [ ] dashboard: add one :)
* [ ] sdk: transfer rate controll
* [ ] svr: file size overflow

### benchmark
---

**the size of each messages is 200, the bench size of messages on each request is 200 too**

* http test

```
$ go run debug/bench/httpserver/server.go
$ make bench-http
go test -v -benchtime 10s -benchmem -bench=Http -run=Nothing github.com/chzyer/fsmq/debug/bench | scripts/addops.awk
testing: warning: no tests to run
PASS
BenchmarkHttpPut	 5000000	      2478 ns/op (403551 op/s) 		      16 B/op	       0 allocs/op
ok  	github.com/chzyer/fsmq/debug/bench	14.875s
```

* sync api (single client wait until server reply)

```
$ make bench-sync-api
go test -v -benchtime 10s -benchmem -bench=ApiSync -run=Nothing github.com/chzyer/fsmq/debug/bench | scripts/addops.awk
testing: warning: no tests to run
PASS
BenchmarkApiSyncGet	 5000000	      2502 ns/op (399680 op/s) 		     332 B/op	       3 allocs/op
BenchmarkApiSyncPut	10000000	      1921 ns/op (520562 op/s) 		       2 B/op	       0 allocs/op
ok  	github.com/chzyer/fsmq/debug/bench	48.126s
```

* internal test (without network)

```
$ make bench-topic
go test -v -benchtime 10s -benchmem -bench=. -run=Nothing github.com/chzyer/fsmq/muxque/topic | scripts/addops.awk
PASS
BenchmarkTopicGet	20000000	      1839 ns/op (543774 op/s) 		 146.24 MB/s	     430 B/op	       5 allocs/op
BenchmarkTopicPut	20000000	      1034 ns/op (967118 op/s) 		 260.06 MB/s	     405 B/op	       2 allocs/op
ok  	github.com/chzyer/fsmq/muxque/topic	73.963s
```

* nsq

```
# using --mem-queue-size=1000000 --data-path= --size=200 --batch-size=200
# compiling/running nsqd
# creating topic/channel
# compiling bench_reader/bench_writer
PUB: [bench_writer] 2015/07/19 20:07:15 duration: 10.003577746s - 29.725mb/s - 155844.243ops/s - 6.417us/op
SUB: [bench_reader] 2015/07/19 20:07:25 duration: 10.014623761s - 17.258mb/s - 90481.282ops/s - 11.052us/op
```
