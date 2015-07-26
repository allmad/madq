# Multiplex Message Queue

[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg)](LICENSE.md)
[![Build Status](https://travis-ci.org/chzyer/mmq.svg?branch=master)](https://travis-ci.org/chzyer/mmq)
[![Coverage Status](https://coveralls.io/repos/chzyer/mmq/badge.svg?branch=master)](https://coveralls.io/r/chzyer/mmq?branch=master)

mmq is design to use in IM, follows pub/sub model, to achieve serve millions users by multiplex sockets.

## goal
* possible to subscribe multiple topics/channels in one client.
* always persist messages to disk to keep them safe
* zero-extra-cost(disk) for multicasted messages/channels
* distributed and decentralized topologies without single points of failure
* the number of topic can be unlimit(depends on hardware)
* provide a horizontally scaling solution

## Benchmark

* http test

```
$ go run github.com/chzyer/mmq/bench/httpserver/* # run the http server
$ go test -benchtime=10s -bench=. github.com/chzyer/mmq/bench
BenchmarkHttpPut	 3000000	      4693 ns/op (aka 213,083 rps)
```

* internal test (without network)

```
$ go test -v -benchmem -bench=. -run=Nothing github.com/chzyer/mmq/topic
PASS
BenchmarkTopicGet	  500000	      3531 ns/op (aka 283,205 rps)	     431 B/op	       7 allocs/op
BenchmarkTopicPut	  500000	      2991 ns/op (aka 334,336 rps) 	     134 B/op	       3 allocs/op
ok  	github.com/chzyer/mmq/topic	4.404s
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
