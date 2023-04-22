package env

import (
	"fmt"
	"io"
	"path/filepath"
	"runtime"
)

func NewMockFs() Fs {
	return &mockFs{}
}

type mockFs struct {
}

func (this *mockFs) GetFileSize(string) (uint64, error) {
	return 0, this.methodError()
}

func (this *mockFs) CreateWriteFile(string) (io.WriteCloser, error) {
	return nil, this.methodError()
}

func (this *mockFs) OpenReadFile(string) (io.ReadCloser, uint64, error) {
	return nil, 0, this.methodError()
}

func (this *mockFs) MoveFile(_, _ string) error {
	return this.methodError()
}

func (this *mockFs) Remove(string) error {
	return this.methodError()
}

func (this *mockFs) methodError() error {
	pc, _, _, _ := runtime.Caller(1)
	methodName := filepath.Base(runtime.FuncForPC(pc).Name())
	return fmt.Errorf("mock method: %v", methodName)
}
