// performance tracing
package ptrace

import (
	"fmt"
	"strconv"
	"strings"
)

func Unit(a int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	n := float64(a)
	unitIdx := 0
	for n > 1024 {
		n /= 1024
		unitIdx++
	}
	nstr := fmt.Sprintf("%.2f", n)
	if strings.HasSuffix(nstr, ".00") {
		nstr = nstr[:len(nstr)-3]
	}
	return nstr + units[unitIdx]
}

func strJSON(n string) ([]byte, error) {
	return []byte(strconv.Quote(n)), nil
}
