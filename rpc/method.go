package rpc

import "github.com/chzyer/muxque/prot"

var (
	MGet    = prot.NewString("get\n")
	MPut    = prot.NewString("put\n")
	MDelete = prot.NewString("delete\n")
	MPing   = prot.NewString("ping\n")
)
