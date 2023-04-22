package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/kdpdev/extsort/internal/extsort"
	"github.com/kdpdev/extsort/internal/extsort/env"
	"github.com/kdpdev/extsort/internal/utils/fs"
	"github.com/kdpdev/extsort/internal/utils/misc"
)

const (
	flagInputFilePath        = "in"
	flagOutputFilePath       = "out"
	flagTempDir              = "temp_dir"
	flagWorkersCount         = "max_workers_count"
	flagChunkCapacity        = "chunk_capacity"
	flagPreferredChunkSizeKb = "preferred_chunk_size_kb"
	flagWorkerReadBufSizeKb  = "worker_read_buf_size_kb"
	flagWorkerWriteBufSizeKb = "worker_write_buf_size_kb"
)

func ConfigFromFlags() (extsort.Config, error) {
	cfg, err := extsort.NewDefaultConfig()
	if err != nil {
		return cfg, err
	}

	flag.StringVar(&cfg.InputFilePath, flagInputFilePath, "", "input file path")
	flag.StringVar(&cfg.OutputFilePath, flagOutputFilePath, "", "output file path")
	flag.StringVar(&cfg.TempDir, flagTempDir, extsort.GetDefaultTempDir(), "temp dir")
	flag.IntVar(&cfg.WorkersCount, flagWorkersCount, extsort.GetDefaultWorkersCount(), "sort/merge workers count")
	flag.IntVar(&cfg.ChunkCapacity, flagChunkCapacity, extsort.DefaultChunkCapacity, "initial chunk capacity")
	preferredChunkSizeKb := flag.Int(flagPreferredChunkSizeKb, extsort.DefaultPreferredChunkSizeKb, "preferred size of chunk")
	workerReadBufSizeKb := flag.Int(flagWorkerReadBufSizeKb, extsort.DefaultWorkerReadBufSizeKb, "worker's read buf size")
	workerWriteBufSizeKb := flag.Int(flagWorkerWriteBufSizeKb, extsort.DefaultWorkerWriteBufSizeKb, "worker's write buf size")

	flag.Parse()

	cfg.PreferredChunkSize = *preferredChunkSizeKb * 1024
	cfg.WorkerReadBufSize = *workerReadBufSizeKb * 1024
	cfg.WorkerWriteBufSize = *workerWriteBufSizeKb * 1024

	formattedNow := time.Now().Format("2006_01_02__15_04_05")
	cfg.OutputFilePath = strings.ReplaceAll(cfg.OutputFilePath, "{TIME}", formattedNow)
	cfg.TempDir = filepath.Join(cfg.TempDir, "extsort_"+formattedNow)

	return cfg, cfg.Check()
}

func main() {
	l := log.Default()
	err := execMain(l)
	if err != nil {
		l.Fatal(err)
		return
	}
	l.Println("DONE")
}

func execMain(l *log.Logger) (err error) {
	logf := l.Printf

	ctx := context.Background()
	ctx = extsort.WithLogger(ctx, logf)
	ctx = extsort.WithFs(ctx, env.NewOsFs())
	ctx = extsort.WithUnhandledErrorLogger(ctx)
	ctx, unhandledErrors := extsort.WithUnhandledErrorsCollector(ctx)
	ctx = extsort.WithUnhandledErrorDecorator(ctx, extsort.DefaultUnhandledErrorDecorator())
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	defer func() {
		errs := unhandledErrors()
		if len(errs) > 0 {
			logf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
			logf("UNHANDLED ERRORS COUNT: %v", len(errs))
			for i, e := range errs {
				logf("UNHANDLED ERROR [%v/%v]: %v", i+1, len(errs), e)
			}
			logf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
		}
	}()

	cfg, err := ConfigFromFlags()
	if err != nil {
		return err
	}

	cfg, cleanupPaths, err := preparePaths(cfg)
	if err != nil {
		return err
	}
	onceErr := misc.NewOnceError(&err)
	defer onceErr.Invoke(cleanupPaths)

	err = cfg.Check()
	if err != nil {
		return err
	}

	return extsort.ExecExtSort(ctx, cfg)
}

func preparePaths(cfg extsort.Config) (extsort.Config, func() error, error) {
	cfg, err := makePathsAbs(cfg)
	if err != nil {
		return cfg, nil, err
	}

	err = fs.EnsureDirCreated(cfg.TempDir)
	if err != nil {
		return cfg, nil, err
	}

	cleanup := func() error {
		return os.RemoveAll(cfg.TempDir)
	}

	return cfg, cleanup, nil
}

func makePathsAbs(cfg extsort.Config) (extsort.Config, error) {
	var err error
	cfg.InputFilePath, err = filepath.Abs(cfg.InputFilePath)
	if err != nil {
		return cfg, err
	}

	cfg.OutputFilePath, err = filepath.Abs(cfg.OutputFilePath)
	if err != nil {
		return cfg, err
	}

	cfg.TempDir, err = filepath.Abs(cfg.TempDir)
	if err != nil {
		return cfg, err
	}

	return cfg, nil
}
