package extsort

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/kdpdev/extsort/internal/utils/alg"
	"github.com/kdpdev/extsort/internal/utils/misc"
)

func ExecExtSort(ctx context.Context, cfg Config) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}

	beginExecution := time.Now().UnixMicro()

	err = cfg.Check()
	if err != nil {
		return err
	}

	ctx = WithCallerScope(ctx)
	ctx, logf := WithPrefixedLogger(ctx, "extsort")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logf("config: %v", misc.ToPrettyString(cfg))

	execInfo := ExecInfoFromConfig(cfg)
	defer misc.InvokeIfNotError(&err, func() {
		logf("Exec info: %v", misc.ToPrettyString(execInfo))
	})

	splittingDuration, chunkFiles, err := misc.MeasureCallRE(func() (_ []string, splittingErr error) {
		splittingCtx, _ := WithPrefixedLogger(ctx, "splitting")

		inputFileSize, splittingErr := GetFs(splittingCtx).GetFileSize(cfg.InputFilePath)
		if splittingErr != nil {
			return nil, splittingErr
		}

		updateProgress, finishProgress := makeSplittingProgress(inputFileSize)
		defer func() { finishProgress(splittingCtx, splittingErr) }()

		opts := SplittingOptions{
			OutputDir:          cfg.TempDir,
			ChunkCapacity:      cfg.ChunkCapacity,
			PreferredChunkSize: cfg.PreferredChunkSize,
			WriteBufSize:       cfg.WorkerWriteBufSize,
			ReadBufSize:        cfg.WorkerReadBufSize,
			WorkersCount:       cfg.WorkersCount,
		}

		return SplitFileToSortedChunks(splittingCtx, cfg.InputFilePath, opts, updateProgress)
	})

	if err != nil {
		return err
	}

	mergingDuration, mergedFilePath, err := misc.MeasureCallRE(func() (_ string, mergingErr error) {
		mergingCtx, _ := WithPrefixedLogger(ctx, "merging")

		opts := MergeOptions{
			OutputDir:    cfg.TempDir,
			ReadBufSize:  cfg.WorkerReadBufSize,
			WriteBufSize: cfg.WorkerWriteBufSize,
			WorkersCount: cfg.WorkersCount,
		}

		updateProgress, finishProgress := makeMergeProgress(uint64(alg.Max(len(chunkFiles)-1, 0)))
		defer func() { finishProgress(mergingCtx, mergingErr) }()

		return Merge(mergingCtx, chunkFiles, opts, updateProgress)
	})
	if err != nil {
		return err
	}

	fs := GetFs(ctx)

	logf("moving: '%v' -> '%v'...", mergedFilePath, cfg.OutputFilePath)
	err = fs.MoveFile(mergedFilePath, cfg.OutputFilePath)
	if err != nil {
		return err
	}
	logf("moving: done")

	execInfo.InputFileSize, err = fs.GetFileSize(cfg.InputFilePath)
	if err != nil {
		return err
	}

	execInfo.OutputFileSize, err = fs.GetFileSize(cfg.OutputFilePath)
	if err != nil {
		return err
	}

	endExecution := time.Now().UnixMicro()
	execInfo.SplittingDuration = splittingDuration
	execInfo.MergingDuration = mergingDuration
	execInfo.ExecDuration = time.Microsecond * time.Duration(endExecution-beginExecution)

	return nil
}

func makeSplittingProgress(max uint64) (update SplittingProgressListener, finish func(ctx context.Context, finishResult error)) {
	logMsgFmt := fmt.Sprintf("progress: %%3v%%%% %%%vv/%v %%v [%%v bytes]", len(fmt.Sprintf("%v", max)), max)
	progress := misc.NewUnsafeProgress(max)
	guard := &sync.Mutex{}

	onUpdate := func(ctx context.Context, chunk StringsChunk, filePath string) error {
		logf := GetLogger(ctx)
		chunkSize := uint64(chunk.SerializedDataSize())

		guard.Lock()
		defer guard.Unlock()

		percents, value, _ := progress.Add(chunkSize)
		logf(logMsgFmt, percents, value, filepath.Base(filePath), chunkSize)

		return nil
	}

	onFinish := makeProgressFinish(guard, &progress)

	return onUpdate, onFinish
}

func makeMergeProgress(max uint64) (update MergingProgressListener, finish func(ctx context.Context, finishResult error)) {
	logMsgFmt := fmt.Sprintf("progress: %%3v%%%% %%%vv/%v %%v|%%v -> %%v", len(fmt.Sprintf("%v", max)), max)
	progress := misc.NewUnsafeProgress(max)
	guard := &sync.Mutex{}

	onUpdate := func(ctx context.Context, out, left, right string) error {
		logf := GetLogger(ctx)

		guard.Lock()
		defer guard.Unlock()

		percents, value, _ := progress.Add(1)
		logf(logMsgFmt, percents, value, filepath.Base(left), filepath.Base(right), filepath.Base(out))

		return nil
	}

	onFinish := makeProgressFinish(guard, &progress)

	return onUpdate, onFinish
}

func makeProgressFinish[T misc.ProgressConstraint](guard *sync.Mutex, progress *misc.Progress[T]) func(ctx context.Context, err error) {
	return func(ctx context.Context, err error) {
		logf := GetLogger(ctx)

		guard.Lock()
		defer guard.Unlock()

		progress.Done()

		if err == nil {
			logf("progress: done")
		} else {
			logf("progress: failed: %v", err)
		}
	}
}
