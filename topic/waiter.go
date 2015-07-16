package topic

type Group struct {
	Name   string
	Offset int
}

// a client only has a waiter
//
// once client want to get messages which are not prepared,
// client will add its `Waiter` to a Topic,
// and wait for messages via stream untils got all it want,
// and then topic will remove waiter from its waiter list.
type Waiter struct {
	// list all topic it subscribe
	List map[string][]*Group
}

func NewWaiter() *Waiter {
	return nil
}
