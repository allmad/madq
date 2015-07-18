package topic

import "github.com/chzyer/mmq/mmq"

// a client has multiple waiters
//
// once client want to get messages which are not prepared,
// client will add its `Waiter` to a Topic,
// and wait for messages via stream untils got all it want,
// and then topic will remove waiter from its waiter list.
type Waiter struct {
	oriOff  int64
	offset  int64
	size    int
	oriSize int
	reply   chan<- []*mmq.Message
}

func (w *Waiter) toGetArg(err chan<- error) *getArgs {
	return &getArgs{
		w.offset, w.size, w.reply, err,
		w.oriOff, w.oriSize,
	}
}
