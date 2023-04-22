package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"

	"github.com/kdpdev/extsort/internal/extsort"
	"github.com/kdpdev/extsort/internal/extsort/env"
	"github.com/kdpdev/extsort/internal/utils/fs"
	"github.com/kdpdev/extsort/internal/utils/misc"
)

func onResult(filePath string, err error) {
	if err == nil {
		log.Printf("Result: SORTED. The '%v' file is sorted", filePath)
		return
	}

	if errors.Is(err, extsort.ErrNotSorted) {
		log.Printf("Result: NOT SORTED. The '%v' file is not sorted", filePath)
		return
	}

	log.Fatalf("FAILED: %v", err)
}

func main() {
	filePath := flag.String("in", "", "file to be checked")
	flag.Parse()

	if *filePath == "" {
		onResult("", fmt.Errorf("%w: input file is not specified", os.ErrInvalid))
		return
	}

	absFilePath, err := filepath.Abs(*filePath)
	if err != nil {
		absFilePath = *filePath
	}

	dur, err := misc.MeasureCallE(func() error {
		return check(absFilePath)
	})
	log.Printf("duration: %v", dur)
	onResult(absFilePath, err)
}

func check(filePath string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	osFs := env.NewOsFs()
	file, fileSize, err := osFs.OpenReadFile(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	updateProgress := makeProgress(fileSize, log.Printf)
	reader := bufio.NewReaderSize(file, extsort.DefaultWorkerReadBufSizeKb*1024)
	linesGen := extsort.NewSyncLinesGenFromReader(ctx, reader)
	prevLine := ""
	linesCount, err := extsort.EnumLines(linesGen, func(line string) error {
		if prevLine > line {
			return extsort.ErrNotSorted
		}
		prevLine = line
		updateProgress(uint64(len(line)))
		return nil
	})

	if err != nil {
		return err
	}

	log.Printf("lines count: %v", linesCount)
	return err
}

func makeProgress(max uint64, logf extsort.Logf) func(size uint64) {
	logMsgFmt := fmt.Sprintf("progress: %%3v%%%% %%%vv/%v %%v", len(fmt.Sprintf("%v", max)), max)
	progress := misc.NewProgress(max, false)
	guard := sync.Mutex{}
	return func(size uint64) {
		guard.Lock()
		defer guard.Unlock()
		percents, value, changed := progress.Add(size + 1) // +1 is because of the line is without end of line = '\n'
		if changed {
			logf(logMsgFmt, percents, value, fs.FormatSileSize(value))
		}
	}
}
