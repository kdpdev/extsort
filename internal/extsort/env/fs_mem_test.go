package env

import (
	"io"
	"os"
	"path"
	"testing"

	"github.com/kdpdev/extsort/internal/utils/tests"
)

func Test_MemFs_CreateWriteFile(t *testing.T) {
	fs := NewMemFs(nil)

	file, err := fs.CreateWriteFile("dir/file")
	tests.CheckErrorIs(t, os.ErrNotExist, err)

	file, err = fs.CreateWriteFile("file")
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, file.Close())
	tests.CheckErrorIs(t, os.ErrClosed, file.Close())

	file, err = fs.CreateWriteFile("file")
	tests.CheckErrorIs(t, os.ErrExist, err)

	tests.CheckExpected(t, false, fs.HasOpenedEntries())
}

func Test_MemFs_OpenReadFile(t *testing.T) {
	fs := NewMemFs(nil)

	rFile, fileSize, err := fs.OpenReadFile("file")
	tests.CheckErrorIs(t, os.ErrNotExist, err)

	wFile, err := fs.CreateWriteFile("file")
	tests.CheckNotError(t, err)

	rFile, fileSize, err = fs.OpenReadFile("file")
	tests.CheckErrorIs(t, os.ErrPermission, err)

	tests.CheckNotError(t, wFile.Close())

	rFile, fileSize, err = fs.OpenReadFile("file")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, uint64(0), fileSize)
	tests.CheckNotError(t, rFile.Close())

	tests.CheckExpected(t, false, fs.HasOpenedEntries())
}

func Test_MemFs_ReadWrite_1(t *testing.T) {
	fs := NewMemFs(nil)

	wFile, err := fs.CreateWriteFile("file")
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, wFile.Close())

	fileSize, err := fs.GetFileSize("file")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, 0, fileSize)

	rFile, size, err := fs.OpenReadFile("file")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, 0, size)
	read, err := rFile.Read(make([]byte, 0))
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, 0, read)
	read, err = rFile.Read(make([]byte, 1))
	tests.CheckErrorIs(t, io.EOF, err)
	tests.CheckExpected(t, 0, read)
	tests.CheckNotError(t, rFile.Close())
	read, err = rFile.Read(make([]byte, 0))
	tests.CheckErrorIs(t, os.ErrClosed, err)
	tests.CheckExpected(t, 0, read)

	tests.CheckExpected(t, false, fs.HasOpenedEntries())
}

func Test_MemFs_ReadWrite_2(t *testing.T) {
	fs := NewMemFs(nil)

	text := "0123456789"

	wFile, err := fs.CreateWriteFile("file")
	tests.CheckNotError(t, err)
	written, err := wFile.Write([]byte(text))
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, len(text), written)
	tests.CheckNotError(t, wFile.Close())

	rFile, size, err := fs.OpenReadFile("file")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, len(text), int(size))
	buf := make([]byte, len(text))
	read, err := rFile.Read(buf)
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, len(text), read)
	tests.CheckExpected(t, text, string(buf))
	read, err = rFile.Read(buf)
	tests.CheckErrorIs(t, io.EOF, err)
	tests.CheckExpected(t, 0, read)
	tests.CheckNotError(t, rFile.Close())

	rFile, size, err = fs.OpenReadFile("file")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, len(text), int(size))
	buf = make([]byte, len(text)+1)
	read, err = rFile.Read(buf)
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, len(text), read)
	tests.CheckExpected(t, text, string(buf[:read]))
	read, err = rFile.Read(buf)
	tests.CheckErrorIs(t, io.EOF, err)
	tests.CheckExpected(t, 0, read)
	tests.CheckNotError(t, rFile.Close())

	tests.CheckExpected(t, false, fs.HasOpenedEntries())
}

func TestMemFs_EnsureDirExists(t *testing.T) {
	fs := NewMemFs(nil)

	badPaths := []string{"", ".", "..", "/", "//", "./", "../", ":"}
	for _, badPath := range badPaths {
		_, err := fs.EnsureDirExists(badPath)
		tests.CheckErrorIs(t, path.ErrBadPattern, err)
	}

	created, err := fs.EnsureDirExists("root")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, true, created)

	created, err = fs.EnsureDirExists("root")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, false, created)

	created, err = fs.EnsureDirExists("root/1/2")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, true, created)

	created, err = fs.EnsureDirExists("root/1/3")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, true, created)

	created, err = fs.EnsureDirExists("root/1")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, false, created)

	file, err := fs.CreateWriteFile("root/1/2/file")
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, file.Close())

	file, err = fs.CreateWriteFile("root/1/file")
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, file.Close())

	created, err = fs.EnsureDirExists("root/1/2/file")
	tests.CheckErrorIs(t, os.ErrInvalid, err)

	created, err = fs.EnsureDirExists("root/1/file")
	tests.CheckErrorIs(t, os.ErrInvalid, err)

	created, err = fs.EnsureDirExists("root/1/file/3")
	tests.CheckErrorIs(t, os.ErrInvalid, err)

	tests.CheckExpected(t, false, fs.HasOpenedEntries())
}

func TestMemFs_Remove(t *testing.T) {
	fs := NewMemFs(nil)

	created, err := fs.EnsureDirExists("root/1/2/3")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, true, created)

	file, err := fs.CreateWriteFile("root/1/file1")
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, file.Close())

	file, err = fs.CreateWriteFile("root/1/2/file2")
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, file.Close())

	file, err = fs.CreateWriteFile("root/1/2/3/file31")
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, file.Close())

	file, err = fs.CreateWriteFile("root/1/2/3/file32")
	tests.CheckNotError(t, err)
	tests.CheckErrorIs(t, os.ErrPermission, fs.Remove("root"))
	tests.CheckNotError(t, file.Close())

	err = fs.Remove("root/1/2/3/file32")
	tests.CheckNotError(t, err)
	_, err = fs.GetFileSize("root/1/2/3/file32")
	tests.CheckErrorIs(t, os.ErrNotExist, err)

	tests.CheckNotError(t, fs.Remove("root/1/2"))
	_, err = fs.GetFileSize("root/1/file1")
	tests.CheckNotError(t, err)
	_, err = fs.GetFileSize("root/1/2/file2")
	tests.CheckErrorIs(t, os.ErrNotExist, err)
	_, err = fs.GetFileSize("root/1/2/3/file3")
	tests.CheckErrorIs(t, os.ErrNotExist, err)

	created, err = fs.EnsureDirExists("root/1/2")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, true, created)

	created, err = fs.EnsureDirExists("root/1/2/3")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, true, created)

	tests.CheckErrorIs(t, os.ErrNotExist, fs.Remove("root/1/2/3/notExists"))

	tests.CheckExpected(t, false, fs.HasOpenedEntries())
}

func TestMemFs_MoveFile(t *testing.T) {
	fs := NewMemFs(nil)

	_, err := fs.EnsureDirExists("root1")
	tests.CheckNotError(t, err)

	_, err = fs.EnsureDirExists("root2")
	tests.CheckNotError(t, err)

	tests.CheckErrorIs(t, os.ErrInvalid, fs.MoveFile("root1", "root2"))
	tests.CheckErrorIs(t, os.ErrNotExist, fs.MoveFile("root1/src", "root2"))
	tests.CheckErrorIs(t, os.ErrNotExist, fs.MoveFile("root1/src", "root2/dst"))

	file, err := fs.CreateWriteFile("root1/src")
	tests.CheckNotError(t, err)
	tests.CheckErrorIs(t, os.ErrPermission, fs.MoveFile("root1/src", "root2"))
	tests.CheckErrorIs(t, os.ErrPermission, fs.MoveFile("root1/src", "root2/dst"))
	tests.CheckNotError(t, file.Close())
	tests.CheckErrorIs(t, os.ErrInvalid, fs.MoveFile("root1/src", "root2"))
	tests.CheckNotError(t, fs.MoveFile("root1/src", "root2/dst"))
	_, err = fs.GetFileSize("root2/dst")
	tests.CheckNotError(t, err)
	tests.CheckErrorIs(t, os.ErrNotExist, fs.MoveFile("root1/src", "root2/dst"))

	file, err = fs.CreateWriteFile("root1/src")
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, file.Close())
	tests.CheckErrorIs(t, os.ErrExist, fs.MoveFile("root1/src", "root2/dst"))
	tests.CheckNotError(t, fs.MoveFile("root1/src", "dst"))
	_, err = fs.GetFileSize("dst")
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, false, fs.HasOpenedEntries())
}
