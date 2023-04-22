package extsort

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/kdpdev/extsort/internal/utils/misc"
)

type SplittingOptions struct {
	OutputDir          string
	ChunkCapacity      int
	PreferredChunkSize int
	WriteBufSize       int
	ReadBufSize        int
	WorkersCount       int
}

type SplittingProgressListener func(ctx context.Context, chunk StringsChunk, filePath string) error

func SplitFileToSortedChunks(
	ctx context.Context,
	inputFile string,
	opts SplittingOptions,
	updateProgress SplittingProgressListener) (chunkFilePaths []string, err error) {

	if err = ctx.Err(); err != nil {
		return nil, err
	}

	ctx = WithCallerScope(ctx)

	onceErr := misc.NewOnceError(&err)
	onceErr = misc.NewOnceEventWithNotSetNotification(onceErr, GetContextedUnhandledErrorHandler(ctx))

	inputFileStream, _, err := GetFs(ctx).OpenReadFile(inputFile)
	if err != nil {
		return nil, err
	}
	defer onceErr.Invoke(inputFileStream.Close)

	return SplitStreamToSortedChunks(ctx, inputFileStream, opts, updateProgress)
}

func SplitStreamToSortedChunks(
	ctx context.Context,
	inputStream io.Reader,
	opts SplittingOptions,
	updateProgress SplittingProgressListener) (chunkFilePaths []string, err error) {

	if err = ctx.Err(); err != nil {
		return nil, err
	}

	if updateProgress == nil {
		updateProgress = func(ctx context.Context, chunk StringsChunk, filePath string) error { return nil }
	}

	ctx = WithCallerScope(ctx)
	ctx = WithUnhandledErrorContextErrorsFilter(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	guard := &sync.RWMutex{}

	onceErr := misc.NewOnceError(&err)
	onceErr = misc.NewOnceEventWithGuard(onceErr, guard)
	onceErr = misc.NewOnceEventWithNotSetNotification(onceErr, GetContextedUnhandledErrorHandler(ctx))

	saveChunk := makeChunksSaver(opts.OutputDir, opts.WriteBufSize)

	handleChunk := func(ctx context.Context, chunk StringsChunk) {
		chunk.Sort()
		filePath, e := saveChunk(ctx, chunk)

		if e == nil {
			e = updateProgress(ctx, chunk, filePath)
		}

		if e != nil {
			if onceErr.TrySet(e) {
				cancel()
			}
			return
		}

		guard.Lock()
		defer guard.Unlock()
		chunkFilePaths = append(chunkFilePaths, filePath)
	}

	enumErr := func() error { // because of the 'defer onceErr.Invoke(chunksProc.Close)', it waits all tasks
		chunksProc := misc.NewAsyncProcessor(opts.WorkersCount)
		defer onceErr.Invoke(chunksProc.Close)
		inputFileReader := bufio.NewReaderSize(inputStream, opts.ReadBufSize)
		return EnumChunks(
			ctx,
			inputFileReader,
			opts.PreferredChunkSize,
			opts.ChunkCapacity,
			func(ctx context.Context, chunk StringsChunk) error {
				return chunksProc.Exec(func() { handleChunk(ctx, chunk) })
			})
	}()

	onceErr.TrySet(enumErr)

	return chunkFilePaths, err
}

func EnumChunks(
	ctx context.Context,
	source io.Reader,
	preferredChunkSize int,
	chunkCapacity int,
	consume func(ctx context.Context, chunk StringsChunk) error) error {

	if err := ctx.Err(); err != nil {
		return err
	}

	if consume == nil {
		return os.ErrInvalid
	}

	newChunk := func() StringsChunk { return NewArrStringsChunk(chunkCapacity) }

	ctx = WithCallerScope(ctx)

	firstChunk := newChunk()
	chunk := firstChunk
	nextLine := NewSyncLinesGenFromReader(ctx, source)
	_, err := EnumLines(nextLine, func(line string) error {
		chunk.Add(line)
		if chunk.SerializedDataSize() >= preferredChunkSize {
			e := consume(ctx, chunk)
			chunk = newChunk()
			return e
		}
		return nil
	})

	if err == nil && (chunk.Len() > 0 || chunk == firstChunk) { // we have prodice at least 1 chunk (event if it is empty)
		err = consume(ctx, chunk)
	}

	if err != nil {
		return err
	}

	return nil
}

func makeChunksSaver(rootDir string, writeBufSize int) func(ctx context.Context, chunk StringsChunk) (string, error) {
	filePathFmt := filepath.Join(rootDir, "chunk_%06v")
	filesPathsGen := misc.MakeSequencedStringsGen(filePathFmt)
	return func(ctx context.Context, chunk StringsChunk) (filePath string, err error) {
		onceErr := misc.NewOnceError(&err)
		onceErr = misc.NewOnceEventWithNotSetNotification(onceErr, GetContextedUnhandledErrorHandler(ctx))

		filePath = filesPathsGen()
		var writer io.WriteCloser
		writer, err = GetFs(ctx).CreateWriteFile(filePath)
		if err != nil {
			return "", err
		}

		defer onceErr.Invoke(writer.Close)

		bufferedWriter := bufio.NewWriterSize(writer, writeBufSize)
		size, err := chunk.Write(bufferedWriter)
		if err != nil {
			return "", err
		}

		err = bufferedWriter.Flush()
		if err != nil {
			return "", err
		}

		if size != chunk.SerializedDataSize() {
			return "", ErrUnexpectedWrittenBytesCount
		}

		return filePath, err
	}
}
