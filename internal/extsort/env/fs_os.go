package env

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/kdpdev/extsort/internal/utils/fs"
)

func NewOsFs() Fs {
	return &osFs{}
}

type osFs struct {
}

func (this *osFs) GetFileSize(filePath string) (uint64, error) {
	file, size, err := this.OpenReadFile(filePath)
	if err == nil {
		err = file.Close()
	}
	return size, err
}

func (this *osFs) CreateWriteFile(filePath string) (io.WriteCloser, error) {
	return fs.CreateWriteOnlyFile(filePath)
}

func (this *osFs) OpenReadFile(filePath string) (io.ReadCloser, uint64, error) {
	return fs.OpenReadOnlyFile(filePath)
}

func (this *osFs) MoveFile(src, dst string) error {
	_, err := fs.EnsureDirExists(filepath.Dir(dst))
	if err != nil {
		return err
	}

	if runtime.GOOS == "windows" {
		from, _ := syscall.UTF16PtrFromString(src)
		to, _ := syscall.UTF16PtrFromString(dst)
		return syscall.MoveFile(from, to)
	}

	return os.Rename(src, dst)
}

func (this *osFs) Remove(entryPath string) error {
	return os.Remove(entryPath)
}
