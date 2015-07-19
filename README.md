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
