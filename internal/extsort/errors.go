package extsort

import (
	"errors"
)

var (
	ErrBadConfig                   = errors.New("bad config")
	ErrNoFiles                     = errors.New("no files")
	ErrNotSorted                   = errors.New("not sorted")
	ErrUnexpectedWrittenBytesCount = errors.New("unexpected written bytes count")
)
