package rpc

var (
	MGet    = NewString("get\n")
	MPut    = NewString("put\n")
	MDelete = NewString("delete\n")
	MPing   = NewString("ping\n")
	MCancel = NewString("cancel\n")
)
