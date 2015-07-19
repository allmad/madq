# Multiplex Message Queue

mmq is design to use in IM, follows pub/sub model, to achieve serve millions users by multiplex sockets.

## goal
* possible to subscribe multiple topics/channels in one client.
* always persist messages to disk to keep them safe
* zero-extra-cost(disk) for multicasted messages/channels
* distributed and decentralized topologies without single points of failure
* the number of topic can be unlimit(depends on hardware)
* provide a horizontally scaling solution

## Benchmark

1. http test
```
$ go run github.com/chzyer/mmq/bench/httpserver/* # run the http server
$ go test -benchtime=10s -bench=. github.com/chzyer/mmq/bench
BenchmarkHttpPut	 3000000	      4693 ns/op (aka 213,083 rps)
```

2. internal test (without network)
```
$ go test -v -benchmem -bench=. -run=Nothing github.com/chzyer/mmq/topic
PASS
BenchmarkTopicGet	  500000	      3598 ns/op	     431 B/op	       7 allocs/op (aka 277,932 rps)
BenchmarkTopicPut	  500000	      3522 ns/op	     134 B/op	       3 allocs/op (aka 283,929 rps)
ok  	github.com/chzyer/mmq/topic	6.795s
```

nsq
```
# using --mem-queue-size=1000000 --data-path= --size=200 --batch-size=200
# compiling/running nsqd
# creating topic/channel
# compiling bench_reader/bench_writer
PUB: [bench_writer] 2015/07/19 19:09:58 duration: 10.000433347s - 25.168mb/s - 131954.282ops/s - 7.578us/op
SUB: [bench_reader] 2015/07/19 19:10:09 duration: 10.008640649s - 16.075mb/s - 84279.577ops/s - 11.865us/op
```
