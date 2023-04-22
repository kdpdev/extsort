package extsort

import (
	"bytes"
	"sort"
	"strings"
	"testing"

	"github.com/kdpdev/extsort/internal/utils/tests"
)

func Test_Chunk(t *testing.T) {
	lines := []string{"xyz", "abc"}
	linesDataSize := 0
	chunk := NewArrStringsChunk(0)
	for _, l := range lines {
		linesDataSize += len(l)
		chunk.Add(l)
	}
	tests.CheckExpected(t, len(lines), chunk.Len())
	tests.CheckExpected(t, linesDataSize+len(lines), chunk.SerializedDataSize())

	chunk.Sort()

	buf := bytes.NewBuffer(nil)
	n, err := chunk.Write(buf)
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, chunk.SerializedDataSize(), n)

	sort.Strings(lines)
	sortedLines := strings.Join(lines, "\n") + "\n"

	tests.CheckExpected(t, sortedLines, buf.String())
}
