package misc

import (
	"fmt"
	"sync"
)

func MakeSequencedStringsGen(strFmt string) func() string {
	guard := &sync.Mutex{}
	id := 0
	return func() string {
		guard.Lock()
		defer guard.Unlock()
		id++
		return fmt.Sprintf(strFmt, id)
	}
}
