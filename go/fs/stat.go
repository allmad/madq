package fs

import (
	"encoding/json"

	"github.com/chzyer/madq/go/common"
)

var Stat GStat

type GStat struct {
	Flusher struct {
		BlockCopy common.Int
	}
}

func (c *GStat) String() string {
	ret, _ := json.MarshalIndent(c, "", "\t")
	return string(ret)
}
