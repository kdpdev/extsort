package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/kdpdev/extsort/internal/extsort"
	"github.com/kdpdev/extsort/internal/extsort/env"
	"github.com/kdpdev/extsort/internal/utils/fs"
	"github.com/kdpdev/extsort/internal/utils/misc"
)

func onResult(filePath string, err error) {
	if err != nil {
		log.Fatalf("FAILED: %v", err)
		return
	}

	log.Printf("DONE: %v", filePath)
}

func main() {
	maxLineLength := flag.Uint("max_line_length", 32, "max line length")
	linesCount := flag.Uint("lines", 1000000, "lines count")
	outputFilePath := flag.String("out", "rand_lines", "output file")

	flag.Parse()

	absOutputFilePath, err := filepath.Abs(*outputFilePath)
	if err != nil {
		absOutputFilePath = *outputFilePath
	}

	osFs := env.NewOsFs()
	file, err := osFs.CreateWriteFile(absOutputFilePath)
	if err != nil {
		onResult(absOutputFilePath, err)
		return
	}
	defer file.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	updateProgress := makeProgress(*linesCount, log.Printf)
	getRandomLine := makeRandomLineGen(*maxLineLength)
	bufWriter := bufio.NewWriterSize(file, 1024*1024)
	for i := uint(0); i < *linesCount && ctx.Err() == nil; i++ {
		line := getRandomLine()
		_, err = bufWriter.WriteString(line)
		if err != nil {
			log.Fatal(err)
			return
		}
		updateProgress(line)
	}

	err = ctx.Err()
	if err != nil {
		log.Print(err)
	}

	err = bufWriter.Flush()
	if err != nil {
		onResult(absOutputFilePath, nil)
		return
	}

	err = file.Close()
	if err != nil {
		onResult(absOutputFilePath, nil)
		return
	}

	fileSize, err := osFs.GetFileSize(absOutputFilePath)
	if err != nil {
		onResult(absOutputFilePath, nil)
		return
	}

	log.Printf("out: %v", absOutputFilePath)
	log.Printf("size: %v", fs.FormatSileSize(fileSize))
	onResult(absOutputFilePath, nil)
}

func makeProgress(max uint, logf extsort.Logf) func(line string) {
	logMsgFmt := fmt.Sprintf("progress: %%3v%%%% %%%vv/%v %%v", len(fmt.Sprintf("%v", max)), max)
	progress := misc.NewProgress(max, false)
	size := uint64(0)
	return func(line string) {
		size += uint64(len(line))
		percents, value, changed := progress.Add(1)
		if changed {
			logf(logMsgFmt, percents, value, fs.FormatSileSize(size))
		}
	}
}

func makeRandomLineGen(maxLen uint) func() string {
	rand.Seed(time.Now().UnixNano())
	var symbols = []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	return func() string {
		l := rand.Intn(int(maxLen)) + 1
		result := make([]byte, l+1)
		for i := range result {
			result[i] = symbols[rand.Intn(len(symbols))]
		}
		result[l] = '\n'
		return string(result)
	}
}
