package fs

import (
	"fmt"
	"io"
	"os"

	"github.com/kdpdev/extsort/internal/utils/misc"
)

func OpenReadOnlyFile(filePath string) (*os.File, uint64, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, 0, err
	}

	defer misc.InvokeIfError(&err, func() {
		file.Close()
	})

	fileSize, err := GetFileSize(file)
	if err != nil {
		return nil, 0, err
	}

	return file, fileSize, nil
}

func CreateWriteOnlyFile(filePath string) (*os.File, error) {
	return os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0644)
}

func GetFileSize(file *os.File) (uint64, error) {
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(stat.Size()), nil
}

func EnsureDirExists(dirPath string) (created bool, err error) {
	fi, err := os.Stat(dirPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return false, fmt.Errorf("os.Stat failed: %w", err)
		}

		err = os.MkdirAll(dirPath, 0744)
		if err != nil {
			return false, fmt.Errorf("os.Mkdir failed: %w", err)
		}

		return true, nil
	}

	fileMode := fi.Mode()
	if !fileMode.IsDir() {
		return false, fmt.Errorf("existing '%v' fs entry is not a dir", dirPath)
	}

	return false, nil
}

func EnsureDirCreated(dirPath string) error {
	created, err := EnsureDirExists(dirPath)
	if err != nil {
		return fmt.Errorf("failed to ensure the '%v' dir exists: %w", dirPath, err)
	}

	if !created {
		return fmt.Errorf("the '%v' is already existed", dirPath)
	}

	return nil
}

func MakeNewWriteFilesGen(filePathFmt string) (func() (io.WriteCloser, string, error), error) {
	return MakeNewWritersGen(filePathFmt, func(filePath string) (io.WriteCloser, error) {
		return CreateWriteOnlyFile(filePath)
	})
}

func MakeNewWritersGen(idFmt string, createWriter func(id string) (io.WriteCloser, error)) (func() (io.WriteCloser, string, error), error) {
	idGen := misc.MakeSequencedStringsGen(idFmt)

	gen := func() (io.WriteCloser, string, error) {
		writerId := idGen()
		writer, err := createWriter(writerId)
		if err != nil {
			return nil, "", err
		}
		return writer, writerId, nil
	}

	return gen, nil
}

func FormatFileSize(fileSize uint64) string {
	const megabyte = 1024 * 1024
	const kilobyte = 1024
	if fileSize > megabyte {
		return fmt.Sprintf("~%v Mb", fileSize/megabyte)
	}
	if fileSize > kilobyte {
		return fmt.Sprintf("~%v Kb", fileSize/kilobyte)
	}
	return fmt.Sprintf("%v b", fileSize)
}
