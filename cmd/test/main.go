package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/kdpdev/extsort/internal/extsort"
	"github.com/kdpdev/extsort/internal/utils/alg"
	"github.com/kdpdev/extsort/internal/utils/fs"
	"github.com/kdpdev/extsort/internal/utils/misc"
)

func onResult(err error) {
	if err != nil {
		log.Fatalf("FAILED: %v", err)
		return
	}
	log.Print("DONE")
}

func main() {
	linesCount := flag.Uint("lines", 10000000, "lines count")
	removeTempDir := flag.Bool("cleanup", false, "remove all generated files and folders")
	flag.Parse()

	dur, err := misc.MeasureCallE(func() error {
		return test(*linesCount, *removeTempDir)
	})

	log.Printf("duration: %v", dur)
	onResult(err)
}

func test(linesCount uint, removeTempDir bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	exePath, err := os.Getwd()
	if err != nil {
		return err
	}

	tempDir, removeTempDirFn, err := ensureTempDirCreated()
	if err != nil {
		return err
	}

	if removeTempDir {
		removeTempDirFn()
	}

	log.Printf("temp dir: %v", tempDir)

	randLinesFile := path.Join(tempDir, "rand_lines")
	sortedLinesFile := path.Join(tempDir, "sorted_lines")

	commands := make([][]string, 0, 0)
	commands = append(commands, []string{
		filepath.Join(exePath, "gen.exe"),
		fmt.Sprintf("-out=%v", randLinesFile),
		fmt.Sprintf("-lines=%v", linesCount)})

	commands = append(commands, []string{
		filepath.Join(exePath, "sort.exe"),
		fmt.Sprintf("-in=%v", randLinesFile),
		fmt.Sprintf("-out=%v", sortedLinesFile),
		fmt.Sprintf("-preferred_chunk_size_kb=%v", alg.Min(alg.Max(linesCount/10/1024, 128), 64*1024)),
		fmt.Sprintf("-temp_dir=%v", tempDir)})

	commands = append(commands, []string{
		filepath.Join(exePath, "check.exe"),
		fmt.Sprintf("-in=" + sortedLinesFile)})

	for _, args := range commands {
		err = run(ctx, args[0], args[1:]...)
		if err != nil {
			return err
		}
	}

	return nil
}

func ensureTempDirCreated() (tempDirPath string, cleanup func(), _ error) {
	tempDir := "extsort_" + time.Now().Format("2006_01_02__15_04_05")
	absTempDir, err := filepath.Abs(tempDir)
	if err != nil {
		absTempDir = tempDir
	}

	created, err := fs.EnsureDirExists(absTempDir)
	if err != nil {
		return "", nil, fmt.Errorf("%w: Failed to create temp dir", err)
	}

	if !created {
		return "", nil, fmt.Errorf("%w: Temp dir %v is already exists", os.ErrExist, absTempDir)
	}

	cleanup = func() {
		e := os.RemoveAll(absTempDir)
		if e != nil {
			log.Printf("Failed to remove %v: %v", absTempDir, e)
		}
	}

	return absTempDir, cleanup, nil
}

func run(ctx context.Context, cmdName string, args ...string) error {
	setupStd := func(writerPtr *io.Writer) (io.Reader, func(), error) {
		pr, pw, err := os.Pipe()
		if err != nil {
			return nil, nil, err
		}

		closeFn := func() {
			_ = pr.Close()
			_ = pw.Close()
		}

		*writerPtr = pw

		return pr, closeFn, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctx, cmdName, args...)

	stdErr, closeStdErr, err := setupStd(&cmd.Stderr)
	if err != nil {
		return err
	}
	defer closeStdErr()

	stdOut, closeStdOut, err := setupStd(&cmd.Stdout)
	if err != nil {
		return err
	}
	defer closeStdOut()

	err = cmd.Start()
	if err != nil {
		return err
	}

	cmdBaseName := filepath.Base(cmdName)
	logf := func(format string, args ...interface{}) {
		fmt.Printf(cmdBaseName+": "+format+"\n", args...)
	}
	stopLogger := startLogger(ctx, logf, stdErr, stdOut)
	defer stopLogger()

	return cmd.Wait()
}

func startLogger(ctx context.Context, logf extsort.Logf, stdErr, stdOut io.Reader) (stop func()) {
	stdErrCh, _ := extsort.NewLinesChan(ctx, stdErr)
	stdOutCh, _ := extsort.NewLinesChan(ctx, stdOut)

	stdCh := misc.MergeChannels(stdErrCh, stdOutCh)
	doneCh := make(chan struct{})
	timeoutsCh := make(chan struct{})

	go func() {
		defer close(doneCh)
		timeouts := 0
		for timeouts < 10 {
			select {
			case line := <-stdCh:
				logf("%s", line)
				timeouts = 0
			case <-timeoutsCh:
				timeouts++
			}
		}
	}()

	startTimeoutsLoop := func() {
		defer close(timeoutsCh)
		for {
			select {
			case <-doneCh:
				return
			case timeoutsCh <- struct{}{}:
				time.Sleep(1 * time.Millisecond)
			}
		}
	}

	onceStop := sync.Once{}

	return func() {
		onceStop.Do(startTimeoutsLoop)
	}
}
