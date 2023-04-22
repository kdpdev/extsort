package extsort

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/kdpdev/extsort/internal/extsort/env"
	"github.com/kdpdev/extsort/internal/utils/tests"
)

func NewTestTools(t *testing.T) *TestTools {
	fs := env.NewMemFs(nil)

	ctx := context.Background()
	ctx = WithLogger(ctx, t.Logf)
	ctx = WithFs(ctx, fs)
	ctx = WithUnhandledErrorLogger(ctx)
	ctx, uhErrs := WithUnhandledErrorsCollector(ctx)
	ctx = WithUnhandledErrorDecorator(ctx, DefaultUnhandledErrorDecorator())

	tools := &TestTools{}
	tools.Ctx = ctx
	tools.Fs = fs
	tools.SplittingOpts = SplittingOptions{OutputDir: "splitRes", WorkersCount: 2}
	tools.MergingOpts = MergeOptions{OutputDir: "mergeRes", WorkersCount: 2}
	tools.Quantum = time.Millisecond * 10
	tools.UnhandledErrs = uhErrs

	created, err := fs.EnsureDirExists(tools.SplittingOpts.OutputDir)
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, true, created)

	created, err = fs.EnsureDirExists(tools.MergingOpts.OutputDir)
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, true, created)

	return tools
}

type TestTools struct {
	Fs            *env.MemFs
	Ctx           context.Context
	SplittingOpts SplittingOptions
	MergingOpts   MergeOptions
	Quantum       time.Duration
	UnhandledErrs func() []error
}

func (this *TestTools) CreateFile(name string, data string) error {
	file, err := this.Fs.CreateWriteFile(name)
	if err != nil {
		return err
	}
	defer file.Close()

	n, err := io.WriteString(file, data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("unexpected writen data")
	}

	err = file.Close()
	if err != nil {
		return err
	}

	return nil
}

func (this *TestTools) CheckPresent(name string) error {
	_, err := this.Fs.GetFileSize(name)
	return err
}

func (this *TestTools) CheckAbsent(name string) error {
	_, err := this.Fs.GetFileSize(name)
	if err == os.ErrNotExist {
		return nil
	}
	if err == nil {
		return fmt.Errorf("unexpected: %v", err)
	}
	return fmt.Errorf("unexpected: %w", err)
}

func (this *TestTools) CheckFileSize(filePath string, expected uint64) error {
	size, err := this.Fs.GetFileSize(filePath)
	if err != nil {
		return err
	}
	if size != expected {
		return fmt.Errorf("unexpected file size: %v; expected: %v", size, expected)
	}
	return nil
}

func (this *TestTools) GetLinesForSplitting(linesCount int) string {
	lines := ""
	for i := 0; i < linesCount; i++ {
		lines += strconv.Itoa(linesCount-i-1) + "\n"
	}
	return lines
}

func (this *TestTools) Sleep(ctx context.Context, dur time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(dur):
		return nil
	}
}
