package env

import (
	"io"
)

type Fs interface {
	GetFileSize(filePath string) (uint64, error)
	CreateWriteFile(filePath string) (io.WriteCloser, error)
	OpenReadFile(filePath string) (io.ReadCloser, uint64, error)
	MoveFile(src, dst string) error
	Remove(entryPath string) error
}
