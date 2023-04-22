package extsort

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/kdpdev/extsort/internal/utils/misc"
)

type MergeOptions struct {
	OutputDir    string
	WriteBufSize int
	ReadBufSize  int
	WorkersCount int
}

type MergingProgressListener func(ctx context.Context, left, right, out string) error

func Merge(
	ctx context.Context,
	files []string,
	opts MergeOptions,
	updateProgress MergingProgressListener) (string, error) {

	if err := ctx.Err(); err != nil {
		return "", err
	}

	ctx = WithCallerScope(ctx)

	mergedFilePath, err := merge(ctx, opts, files, updateProgress)
	if err != nil {
		return "", err
	}

	return mergedFilePath, nil
}

func merge(
	ctx context.Context,
	opts MergeOptions,
	files []string,
	updateProgress MergingProgressListener) (_ string, err error) {

	if err = ctx.Err(); err != nil {
		return "", err
	}

	if len(files) == 0 {
		return "", ErrNoFiles
	}

	if updateProgress == nil {
		updateProgress = func(ctx context.Context, left, right, out string) error { return nil }
	}

	getMergedFilePath := misc.MakeSequencedStringsGen(filepath.Join(opts.OutputDir, "merged_%06v"))

	if len(files) == 1 {
		mergedFilePath := getMergedFilePath()
		return mergedFilePath, GetFs(ctx).MoveFile(files[0], mergedFilePath)
	}

	ctx = WithCallerScope(ctx)
	ctx = WithUnhandledErrorContextErrorsFilter(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	onceErr := misc.NewOnceError(&err)
	onceErr = misc.NewOnceEventWithNotSetNotification(onceErr, GetContextedUnhandledErrorHandler(ctx))
	onceErr = misc.NewOnceEventWithGuard(onceErr, nil)

	onError := func(e error) {
		if onceErr.TrySet(e) {
			cancel()
		}
	}

	proc := misc.NewAsyncProcessor(opts.WorkersCount)
	defer onceErr.Invoke(proc.Close)

	var mergeImpl func(files []string) <-chan string
	mergeImpl = func(files []string) <-chan string {

		if len(files) == 0 {
			onError(ErrNoFiles)
			result := make(chan string)
			close(result)
			return result
		}

		if len(files) == 1 {
			result := make(chan string, 1)
			result <- files[0]
			close(result)
			return result
		}

		lhsChan := mergeImpl(files[:len(files)/2])
		rhsChan := mergeImpl(files[len(files)/2:])

		resultChan := make(chan string, 1)
		closeResultChan := true
		defer misc.InvokeIfTrue(&closeResultChan, func() {
			close(resultChan)
		})

		processErr := proc.Exec(func() {
			defer close(resultChan)

			waitForResult := func(from <-chan string) string {
				select {
				case <-ctx.Done():
					return ""
				case res := <-from:
					return res
				}
			}

			lhsResult := waitForResult(lhsChan)
			if lhsResult == "" {
				return
			}

			rhsResult := waitForResult(rhsChan)
			if rhsResult == "" {
				return
			}

			mergedFilePath := getMergedFilePath()
			mergeErr := MergeFiles(ctx, opts, lhsResult, rhsResult, mergedFilePath)
			if mergeErr != nil {
				onError(mergeErr)
				return
			}

			mergeErr = updateProgress(ctx, mergedFilePath, lhsResult, rhsResult)
			if mergeErr != nil {
				onError(mergeErr)
				return
			}

			resultChan <- mergedFilePath
		})

		if processErr != nil {
			onError(processErr)
			return resultChan
		}

		closeResultChan = false

		return resultChan
	}

	mergedFilePath := <-mergeImpl(files)

	onceErr.TrySet(ctx.Err())

	return mergedFilePath, err
}

func MergeFiles(
	ctx context.Context,
	opts MergeOptions,
	leftFilePath string,
	rightFilePath string,
	targetFilePath string) (err error) {

	if err = ctx.Err(); err != nil {
		return err
	}

	if leftFilePath == "" || rightFilePath == "" {
		return ErrNoFiles
	}

	ctx = WithCallerScope(ctx)

	onceErr := misc.NewOnceError(&err)
	onceErr = misc.NewOnceEventWithNotSetNotification(onceErr, GetContextedUnhandledErrorHandler(ctx))

	fs := GetFs(ctx)

	closeLeft := true
	left, _, err := fs.OpenReadFile(leftFilePath)
	if err != nil {
		return err
	}
	defer misc.InvokeIfTrue(&closeLeft, func() {
		onceErr.TrySet(left.Close())
	})

	closeRight := true
	right, _, err := fs.OpenReadFile(rightFilePath)
	if err != nil {
		return err
	}
	defer misc.InvokeIfTrue(&closeRight, func() {
		onceErr.TrySet(right.Close())
	})

	target, err := fs.CreateWriteFile(targetFilePath)
	if err != nil {
		return err
	}

	defer func() {
		e := target.Close()
		if e != nil {
			if err == nil {
				err = e
			} else {
				OnUnhandledError(ctx, e)
			}
		}

		if err != nil {
			e = fs.Remove(targetFilePath)
			if e != nil {
				OnUnhandledError(ctx, e)
			}
		}
	}()

	leftReader := bufio.NewReaderSize(left, opts.ReadBufSize)
	rightReader := bufio.NewReaderSize(right, opts.ReadBufSize)
	targetWriter := bufio.NewWriterSize(target, opts.WriteBufSize)

	err = MergeStreams(ctx, leftReader, rightReader, targetWriter)
	if err != nil {
		return err
	}

	closeLeft = false
	if err = left.Close(); err == nil {
		err = fs.Remove(leftFilePath)
	}
	if err != nil {
		return err
	}

	closeRight = false
	if err = right.Close(); err == nil {
		err = fs.Remove(rightFilePath)
	}
	if err != nil {
		return err
	}

	return nil
}

func MergeStreams(ctx context.Context, leftReader, rightReader io.Reader, out *bufio.Writer) (resultError error) {
	if resultError = ctx.Err(); resultError != nil {
		return resultError
	}

	if leftReader == rightReader {
		return os.ErrInvalid
	}

	defer misc.InvokeIfNotError(&resultError, func() {
		resultError = out.Flush()
	})

	endOfLine := byte('\n')

	writeLine := func(line string) error {
		n, err := out.WriteString(line)
		if err == nil {
			err = out.WriteByte(endOfLine)
			n += 1
		}
		if err != nil {
			return err
		}

		if n != len(line)+1 {
			return ErrUnexpectedWrittenBytesCount
		}

		return nil
	}

	writeRest := func(getLine func() (string, bool, error)) error {
		for {
			line, done, err := getLine()
			if done {
				return err
			}
			err = writeLine(line)
			if err != nil {
				return err
			}
		}
	}

	// NOTE: in case of async readers Context with Cancel is needed
	getLeftLine := NewSyncLinesGenFromReader(ctx, leftReader)
	getRightLine := NewSyncLinesGenFromReader(ctx, rightReader)

	leftLine, done, err := getLeftLine()
	if done {
		if err != nil {
			return err
		}

		return writeRest(getRightLine)
	}

	rightLine, done, err := getRightLine()
	if done {
		if err != nil {
			return err
		}

		err = writeLine(leftLine)
		if err != nil {
			return err
		}

		return writeRest(getLeftLine)
	}

	for {
		if leftLine < rightLine {
			err = writeLine(leftLine)
			if err != nil {
				return err
			}

			leftLine, done, err = getLeftLine()
			if done {
				if err != nil {
					return err
				}

				err = writeLine(rightLine)
				if err != nil {
					return err
				}

				return writeRest(getRightLine)
			}
		} else {
			err = writeLine(rightLine)
			if err != nil {
				return err
			}

			rightLine, done, err = getRightLine()
			if done {
				if err != nil {
					return err
				}

				err = writeLine(leftLine)
				if err != nil {
					return err
				}

				return writeRest(getLeftLine)
			}
		}
	}
}
