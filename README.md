# Multiplex Message Queue

mmq is design to use in IM, follows pub/sub model, to achieve serve by millions of users indirectly.

## goal
* possible to subscribe multiple topics/channels in one client.
* always persist messages to disk to keep them safe
* zero-extra-cost(disk) for multicast the messages/channel
* distributed and decentralized topologies without single points of failure
* the number of topic can be unlimit(depends on hardware)
* using a reliable protocol to make sure the process of messages is actually done.
* high scalability
