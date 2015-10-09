package topic

import "github.com/chzyer/fsmq/rpc"

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
	reply   rpc.ReplyChan
}

func newWaiter(arg *getArgs, offset int64, size int) *Waiter {
	return &Waiter{
		offset: offset,
		size:   arg.size,
		reply:  arg.reply,

		oriOff:  arg.oriOff,
		oriSize: arg.oriSize,
	}
}

func (w *Waiter) Equal(get *getArgs) bool {
	return get.oriOff == w.oriOff && get.oriSize == w.oriSize && get.reply == w.reply
}

func (w *Waiter) toGetArg(err chan<- error) *getArgs {
	return &getArgs{
		w.offset, w.size, w.reply, err,
		w.oriOff, w.oriSize,
	}
}
