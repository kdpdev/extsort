package extsort

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kdpdev/extsort/internal/utils/tests"
)

func Test_ExtSort_1(t *testing.T) {
	tools, cfg := newExtSortTools(t)
	tests.CheckErrorIs(t, os.ErrNotExist, ExecExtSort(tools.Ctx, cfg))
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_ExtSort_2(t *testing.T) {
	tools, cfg := newExtSortTools(t)
	tests.CheckNotError(t, tools.CreateFile(cfg.InputFilePath, ""))
	tests.CheckNotError(t, ExecExtSort(tools.Ctx, cfg))
	tests.CheckNotError(t, tools.CheckFileSize(cfg.OutputFilePath, 0))
	tests.CheckNotError(t, tools.CheckFileSize(cfg.InputFilePath, 0))
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_ExtSort_3(t *testing.T) {
	getLines := func(count int) []string {
		lines := make([]string, 0, count)
		for i := count; i > 0; i-- {
			lines = append(lines, fmt.Sprintf("%010v", i))
		}
		return lines
	}

	tools, cfg := newExtSortTools(t)

	linesArr := getLines(5000)
	linesTxt := strings.Join(linesArr, "\n") + "\n"
	tests.CheckNotError(t, tools.CreateFile(cfg.InputFilePath, linesTxt))
	tests.CheckNotError(t, tools.CheckFileSize(cfg.InputFilePath, uint64(len(linesTxt))))

	cfg.WorkerWriteBufSize = 1024
	cfg.WorkerReadBufSize = 1024
	cfg.ChunkCapacity = 1024
	cfg.PreferredChunkSize = 1024
	tests.CheckNotError(t, ExecExtSort(tools.Ctx, cfg))
	tests.CheckNotError(t, tools.CheckFileSize(cfg.OutputFilePath, uint64(len(linesTxt))))

	merged, size, err := tools.Fs.OpenReadFile(cfg.OutputFilePath)
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, uint64(len(linesTxt)), size)

	mergedData, err := ioutil.ReadAll(merged)
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, merged.Close())

	mergedStr := string(mergedData)

	sort.Strings(linesArr)
	linesTxt = strings.Join(linesArr, "\n") + "\n"

	tests.CheckExpected(t, linesTxt, mergedStr)

	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_ExtSort_Cancel_1(t *testing.T) {
	getLines := func(count int) []string {
		lines := make([]string, 0, count)
		for i := count; i > 0; i-- {
			lines = append(lines, fmt.Sprintf("%010v", i))
		}
		return lines
	}

	tools, cfg := newExtSortTools(t)

	linesArr := getLines(5000)
	linesTxt := strings.Join(linesArr, "\n") + "\n"
	tests.CheckNotError(t, tools.CreateFile(cfg.InputFilePath, linesTxt))
	tests.CheckNotError(t, tools.CheckFileSize(cfg.InputFilePath, uint64(len(linesTxt))))

	fatalTimer := time.AfterFunc(tools.Quantum*time.Duration(100), func() {
		t.Fatalf("timed out")
	})
	defer fatalTimer.Stop()

	ctx, cancel := context.WithCancel(tools.Ctx)
	defer cancel()
	cancelTimer := time.AfterFunc(tools.Quantum*5, cancel)
	defer cancelTimer.Stop()

	oldLogf := GetLogger(tools.Ctx)
	newLogf := func(format string, args ...interface{}) {
		oldLogf(format, args...)
		_ = tools.Sleep(ctx, tools.Quantum)
	}
	ctx = WithLogger(ctx, newLogf)

	cfg.WorkerWriteBufSize = 1024
	cfg.WorkerReadBufSize = 1024
	cfg.ChunkCapacity = 1024
	cfg.PreferredChunkSize = 1024
	tests.CheckErrorIs(t, context.Canceled, ExecExtSort(ctx, cfg))
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_ExtSort_Cancel_2(t *testing.T) {
	getLines := func(count int) []string {
		lines := make([]string, 0, count)
		for i := count; i > 0; i-- {
			lines = append(lines, fmt.Sprintf("%010v", i))
		}
		return lines
	}

	tools, cfg := newExtSortTools(t)

	linesArr := getLines(5000)
	linesTxt := strings.Join(linesArr, "\n") + "\n"
	tests.CheckNotError(t, tools.CreateFile(cfg.InputFilePath, linesTxt))
	tests.CheckNotError(t, tools.CheckFileSize(cfg.InputFilePath, uint64(len(linesTxt))))

	fatalTimer := time.AfterFunc(tools.Quantum*time.Duration(200), func() {
		t.Fatalf("timed out")
	})
	defer fatalTimer.Stop()

	ctx, cancel := context.WithCancel(tools.Ctx)
	defer cancel()
	cancelTimer := time.AfterFunc(tools.Quantum*100, cancel)
	defer cancelTimer.Stop()

	oldLogf := GetLogger(tools.Ctx)
	newLogf := func(format string, args ...interface{}) {
		oldLogf(format, args...)
		_ = tools.Sleep(ctx, tools.Quantum)
	}
	ctx = WithLogger(ctx, newLogf)

	cfg.WorkerWriteBufSize = 1024
	cfg.WorkerReadBufSize = 1024
	cfg.ChunkCapacity = 1024
	cfg.PreferredChunkSize = 1024
	tests.CheckErrorIs(t, context.Canceled, ExecExtSort(ctx, cfg))
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_ExtSort_Timeout_1(t *testing.T) {
	getLines := func(count int) []string {
		lines := make([]string, 0, count)
		for i := count; i > 0; i-- {
			lines = append(lines, fmt.Sprintf("%010v", i))
		}
		return lines
	}

	tools, cfg := newExtSortTools(t)

	linesArr := getLines(5000)
	linesTxt := strings.Join(linesArr, "\n") + "\n"
	tests.CheckNotError(t, tools.CreateFile(cfg.InputFilePath, linesTxt))
	tests.CheckNotError(t, tools.CheckFileSize(cfg.InputFilePath, uint64(len(linesTxt))))

	fatalTimer := time.AfterFunc(tools.Quantum*time.Duration(100), func() {
		t.Fatalf("timed out")
	})
	defer fatalTimer.Stop()

	ctx, cancel := context.WithTimeout(tools.Ctx, tools.Quantum*5)
	defer cancel()

	oldLogf := GetLogger(tools.Ctx)
	newLogf := func(format string, args ...interface{}) {
		oldLogf(format, args...)
		_ = tools.Sleep(ctx, tools.Quantum)
	}
	ctx = WithLogger(ctx, newLogf)

	cfg.WorkerWriteBufSize = 1024
	cfg.WorkerReadBufSize = 1024
	cfg.ChunkCapacity = 1024
	cfg.PreferredChunkSize = 1024
	tests.CheckErrorIs(t, context.DeadlineExceeded, ExecExtSort(ctx, cfg))
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_ExtSort_Timeout_2(t *testing.T) {
	getLines := func(count int) []string {
		lines := make([]string, 0, count)
		for i := count; i > 0; i-- {
			lines = append(lines, fmt.Sprintf("%010v", i))
		}
		return lines
	}

	tools, cfg := newExtSortTools(t)

	linesArr := getLines(5000)
	linesTxt := strings.Join(linesArr, "\n") + "\n"
	tests.CheckNotError(t, tools.CreateFile(cfg.InputFilePath, linesTxt))
	tests.CheckNotError(t, tools.CheckFileSize(cfg.InputFilePath, uint64(len(linesTxt))))

	fatalTimer := time.AfterFunc(tools.Quantum*time.Duration(200), func() {
		t.Fatalf("timed out")
	})
	defer fatalTimer.Stop()

	ctx, cancel := context.WithTimeout(tools.Ctx, tools.Quantum*100)
	defer cancel()

	oldLogf := GetLogger(tools.Ctx)
	newLogf := func(format string, args ...interface{}) {
		oldLogf(format, args...)
		_ = tools.Sleep(ctx, tools.Quantum)
	}
	ctx = WithLogger(ctx, newLogf)

	cfg.WorkerWriteBufSize = 1024
	cfg.WorkerReadBufSize = 1024
	cfg.ChunkCapacity = 1024
	cfg.PreferredChunkSize = 1024
	tests.CheckErrorIs(t, context.DeadlineExceeded, ExecExtSort(ctx, cfg))
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func newExtSortTools(t *testing.T) (*TestTools, Config) {
	tools := NewTestTools(t)
	cfg, err := NewDefaultConfig()
	tests.CheckNotError(t, err)

	cfg.TempDir = "temp"
	created, err := tools.Fs.EnsureDirExists(cfg.TempDir)
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, true, created)

	return tools, cfg
}
