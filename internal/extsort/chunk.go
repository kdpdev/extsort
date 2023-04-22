package extsort

import (
	"io"
	"sort"

	"github.com/kdpdev/extsort/internal/utils/alg"
)

type StringsChunk interface {
	Add(s string)
	SerializedDataSize() int
	Len() int
	Sort()
	Write(w io.Writer) (int, error)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type ArrStringsChunk struct {
	storage  []string
	dataSize int
}

func NewArrStringsChunk(capacity int) *ArrStringsChunk {
	storage := make([]string, 0, alg.Max(capacity, 0))
	return &ArrStringsChunk{
		storage: storage,
	}
}

func (this *ArrStringsChunk) Get(idx int) string {
	return this.storage[idx]
}

func (this *ArrStringsChunk) Add(s string) {
	this.storage = append(this.storage, s)
	this.dataSize += len(s)
}

func (this *ArrStringsChunk) SerializedDataSize() int {
	return this.dataSize + len(this.storage)
}

func (this *ArrStringsChunk) Len() int {
	return len(this.storage)
}

func (this *ArrStringsChunk) Sort() {
	sort.Strings(this.storage)
}

func (this *ArrStringsChunk) IsSorted() bool {
	return sort.SliceIsSorted(*this, func(i, j int) bool {
		return this.storage[i] < this.storage[j]
	})
}

func (this *ArrStringsChunk) Write(w io.Writer) (int, error) {
	endOfLine := []byte{'\n'}
	written := 0
	for _, line := range this.storage {
		n, err := io.WriteString(w, line)
		written += n
		if err == nil {
			n2, err2 := w.Write(endOfLine)
			written += n2
			n += n2
			err = err2
		}
		if err != nil {
			return written, err
		}
		if n != len(line)+1 {
			return written, ErrUnexpectedWrittenBytesCount
		}
	}

	return written, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
