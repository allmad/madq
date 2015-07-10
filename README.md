# Multiplex Message Queue

mmq is design to use in IM, follows pub/sub model, to achieve serve by millions of users indirectly.

## goal
* possible to subscribe multiple topics/channels in one client.
* always persist messages to disk to keep them safe
* zero-extra-cost(disk) for multicasted messages/channels
* distributed and decentralized topologies without single points of failure
* the number of topic can be unlimit(depends on hardware)
* every messages must accepted ack before mark done, auto requeue if message process timeout
* high scalability
