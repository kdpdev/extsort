package env

import (
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/kdpdev/extsort/internal/utils/fs"
	"github.com/kdpdev/extsort/internal/utils/misc"
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
		srcVol := filepath.VolumeName(src)
		dstVol := filepath.VolumeName(dst)
		if srcVol != dstVol {
			err = this.CopyFile(src, dst)
			if err == nil {
				err = this.Remove(src)
			}
			return err
		}
	}

	return os.Rename(src, dst)
}

func (this *osFs) Remove(entryPath string) error {
	return os.Remove(entryPath)
}

func (this *osFs) CopyFile(src, dst string) (err error) {
	onceErr := misc.NewOnceError(&err)

	srcFile, _, err := this.OpenReadFile(src)
	if err != nil {
		return err
	}
	defer onceErr.Invoke(srcFile.Close)

	dstFile, err := this.CreateWriteFile(dst)
	if err != nil {
		return err
	}
	defer onceErr.Invoke(dstFile.Close)

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return err
	}

	return nil
}
