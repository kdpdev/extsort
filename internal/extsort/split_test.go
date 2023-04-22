package extsort

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/kdpdev/extsort/internal/utils/tests"
)

func Test_EnumChunks(t *testing.T) {
	ctx := context.Background()

	buf := bytes.NewBufferString("")
	chunks := make([]StringsChunk, 0)
	err := EnumChunks(ctx, buf, 0, 0, func(ctx context.Context, chunk StringsChunk) error {
		tests.CheckExpected(t, 0, chunk.Len())
		chunks = append(chunks, chunk)
		return nil
	})
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, 1, len(chunks))

	buf = bytes.NewBufferString("\n")
	chunks = make([]StringsChunk, 0)
	err = EnumChunks(ctx, buf, 0, 0, func(ctx context.Context, chunk StringsChunk) error {
		tests.CheckExpected(t, 1, chunk.Len())
		chunks = append(chunks, chunk)
		return nil
	})
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, 1, len(chunks))

	buf = bytes.NewBufferString("1")
	chunks = make([]StringsChunk, 0)
	err = EnumChunks(ctx, buf, 0, 0, func(ctx context.Context, chunk StringsChunk) error {
		tests.CheckExpected(t, 1, chunk.Len())
		chunks = append(chunks, chunk)
		return nil
	})
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, 1, len(chunks))

	buf = bytes.NewBufferString("1\n2")
	chunks = make([]StringsChunk, 0)
	err = EnumChunks(ctx, buf, 0, 0, func(ctx context.Context, chunk StringsChunk) error {
		tests.CheckExpected(t, 1, chunk.Len())
		chunks = append(chunks, chunk)
		return nil
	})
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, 2, len(chunks))
}

func Test_EnumChunks_Cancel_1(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	buf := bytes.NewBufferString("abc")
	err := EnumChunks(ctx, buf, 0, 0, func(ctx context.Context, chunk StringsChunk) error {
		return fmt.Errorf("unexpected")
	})
	tests.CheckErrorIs(t, context.Canceled, err)
}

func Test_EnumChunks_Cancel_2(t *testing.T) {
	tools := NewTestTools(t)

	fatalTimer := time.AfterFunc(tools.Quantum*10, func() {
		t.Fatalf("timed out")
	})
	defer fatalTimer.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelTimer := time.AfterFunc(tools.Quantum*2, cancel)
	defer cancelTimer.Stop()

	buf := bytes.NewBufferString("1\n2\n3\n4\n5\n6\n7\n8\n9\n")
	err := EnumChunks(ctx, buf, 0, 0, func(ctx context.Context, chunk StringsChunk) error {
		return tools.Sleep(ctx, tools.Quantum)
	})
	tests.CheckErrorIs(t, context.Canceled, err)
}

func Test_EnumChunks_Timeout_1(t *testing.T) {
	tools := NewTestTools(t)

	ctx, cancel := context.WithTimeout(context.Background(), tools.Quantum*2)
	defer cancel()
	<-ctx.Done()
	buf := bytes.NewBufferString("abc")
	err := EnumChunks(ctx, buf, 0, 0, func(ctx context.Context, chunk StringsChunk) error {
		return fmt.Errorf("unexpected")
	})
	tests.CheckErrorIs(t, context.DeadlineExceeded, err)
}

func Test_EnumChunks_Timeout_2(t *testing.T) {
	tools := NewTestTools(t)

	fatalTimer := time.AfterFunc(tools.Quantum*10, func() {
		t.Fatalf("timed out")
	})
	defer fatalTimer.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), tools.Quantum*2)
	defer cancel()

	buf := bytes.NewBufferString("1\n2\n3\n4\n5\n6\n7\n8\n9\n")
	err := EnumChunks(ctx, buf, 0, 0, func(ctx context.Context, chunk StringsChunk) error {
		return tools.Sleep(ctx, tools.Quantum)
	})
	tests.CheckErrorIs(t, context.DeadlineExceeded, err)
}

func Test_SplitFile_1(t *testing.T) {
	tools := NewTestTools(t)

	_, err := SplitFileToSortedChunks(tools.Ctx, "input", tools.SplittingOpts, nil)
	tests.CheckErrorIs(t, os.ErrNotExist, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_SplitFile_2(t *testing.T) {
	tools := NewTestTools(t)

	file, err := tools.Fs.CreateWriteFile("input")
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, file.Close())

	files, err := SplitFileToSortedChunks(tools.Ctx, "input", tools.SplittingOpts, nil)
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, 1, len(files))
	tests.CheckNotError(t, tools.CheckFileSize(files[0], 0))

	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_SplitFile_3(t *testing.T) {
	tools := NewTestTools(t)

	linesCount := 10
	expectedChunksCount := 10
	expectedChunkSize := 2 // valid for lines in range 0...9
	tools.SplittingOpts.PreferredChunkSize = expectedChunkSize

	tests.CheckNotError(t, tools.CreateFile("input", tools.GetLinesForSplitting(linesCount)))
	files, err := SplitFileToSortedChunks(tools.Ctx, "input", tools.SplittingOpts, nil)
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, expectedChunksCount, len(files))

	for _, f := range files {
		chunk, fileSize, err := tools.Fs.OpenReadFile(f)
		tests.CheckNotError(t, err)
		tests.CheckExpected(t, uint64(expectedChunkSize), fileSize)
		lines, err := CollectLines(NewSyncLinesGenFromReader(tools.Ctx, chunk))
		tests.CheckNotError(t, err)
		sorted := sort.SliceIsSorted(lines, func(i, j int) bool {
			return lines[i] < lines[j]
		})
		tests.CheckExpected(t, true, sorted)
		tests.CheckNotError(t, chunk.Close())
	}

	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_SplitFile_4(t *testing.T) {
	tools := NewTestTools(t)

	linesCount := 10
	expectedChunksCount := 2
	expectedChunkSize := 2 * (linesCount / expectedChunksCount) // valid for lines in range 0...9
	tools.SplittingOpts.PreferredChunkSize = expectedChunkSize

	tests.CheckNotError(t, tools.CreateFile("input", tools.GetLinesForSplitting(linesCount)))
	files, err := SplitFileToSortedChunks(tools.Ctx, "input", tools.SplittingOpts, nil)
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, expectedChunksCount, len(files))

	for _, f := range files {
		chunk, fileSize, err := tools.Fs.OpenReadFile(f)
		tests.CheckNotError(t, err)
		tests.CheckExpected(t, uint64(expectedChunkSize), fileSize)
		lines, err := CollectLines(NewSyncLinesGenFromReader(tools.Ctx, chunk))
		tests.CheckNotError(t, err)
		sorted := sort.SliceIsSorted(lines, func(i, j int) bool {
			return lines[i] < lines[j]
		})
		tests.CheckExpected(t, true, sorted)
		tests.CheckNotError(t, chunk.Close())
	}

	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_SplitFile_5(t *testing.T) {
	tools := NewTestTools(t)

	linesCount := 10
	expectedChunksCount := 1
	expectedChunkSize := 2 * (linesCount / expectedChunksCount) // valid for lines in range 0...9
	tools.SplittingOpts.PreferredChunkSize = expectedChunkSize

	tests.CheckNotError(t, tools.CreateFile("input", tools.GetLinesForSplitting(linesCount)))
	files, err := SplitFileToSortedChunks(tools.Ctx, "input", tools.SplittingOpts, nil)
	tests.CheckNotError(t, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, expectedChunksCount, len(files))

	for _, f := range files {
		chunk, fileSize, err := tools.Fs.OpenReadFile(f)
		tests.CheckNotError(t, err)
		tests.CheckExpected(t, uint64(expectedChunkSize), fileSize)
		lines, err := CollectLines(NewSyncLinesGenFromReader(tools.Ctx, chunk))
		tests.CheckNotError(t, err)
		sorted := sort.SliceIsSorted(lines, func(i, j int) bool {
			return lines[i] < lines[j]
		})
		tests.CheckExpected(t, true, sorted)
		tests.CheckNotError(t, chunk.Close())
	}

	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_SplitFile_Cancel_1(t *testing.T) {
	tools := NewTestTools(t)

	ctx, cancel := context.WithCancel(tools.Ctx)
	cancel()
	_, err := SplitFileToSortedChunks(ctx, "input", tools.SplittingOpts, func(ctx context.Context, chunk StringsChunk, filePath string) error {
		return fmt.Errorf("unexpected")
	})

	tests.CheckErrorIs(t, context.Canceled, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_SplitFile_Cancel_2(t *testing.T) {
	tools := NewTestTools(t)

	linesCount := 10
	tests.CheckNotError(t, tools.CreateFile("input", tools.GetLinesForSplitting(linesCount)))

	ctx, cancel := context.WithCancel(tools.Ctx)
	cancel()
	_, err := SplitFileToSortedChunks(ctx, "input", tools.SplittingOpts, func(ctx context.Context, chunk StringsChunk, filePath string) error {
		return fmt.Errorf("unexpected")
	})

	tests.CheckErrorIs(t, context.Canceled, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_SplitFile_Cancel_3(t *testing.T) {
	tools := NewTestTools(t)

	linesCount := 10
	tests.CheckNotError(t, tools.CreateFile("input", tools.GetLinesForSplitting(linesCount)))

	fatalTimer := time.AfterFunc(tools.Quantum*time.Duration(linesCount+1), func() {
		t.Fatalf("timed out")
	})
	defer fatalTimer.Stop()

	ctx, cancel := context.WithCancel(tools.Ctx)
	defer cancel()
	cancelTimer := time.AfterFunc(tools.Quantum*2, cancel)
	defer cancelTimer.Stop()

	_, err := SplitFileToSortedChunks(ctx, "input", tools.SplittingOpts, func(ctx context.Context, chunk StringsChunk, filePath string) error {
		return tools.Sleep(ctx, tools.Quantum)
	})

	tests.CheckErrorIs(t, context.Canceled, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_SplitFile_Timeout_1(t *testing.T) {
	tools := NewTestTools(t)

	ctx, cancel := context.WithTimeout(tools.Ctx, tools.Quantum*2)
	defer cancel()
	<-ctx.Done()

	_, err := SplitFileToSortedChunks(ctx, "input", tools.SplittingOpts, func(ctx context.Context, chunk StringsChunk, filePath string) error {
		return fmt.Errorf("unexpected")
	})

	tests.CheckErrorIs(t, context.DeadlineExceeded, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_SplitFile_Timeout_2(t *testing.T) {
	tools := NewTestTools(t)

	linesCount := 10
	tests.CheckNotError(t, tools.CreateFile("input", tools.GetLinesForSplitting(linesCount)))

	ctx, cancel := context.WithTimeout(tools.Ctx, tools.Quantum*2)
	defer cancel()
	<-ctx.Done()

	_, err := SplitFileToSortedChunks(ctx, "input", tools.SplittingOpts, func(ctx context.Context, chunk StringsChunk, filePath string) error {
		return fmt.Errorf("unexpected")
	})

	tests.CheckErrorIs(t, context.DeadlineExceeded, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_SplitFile_Timeout_3(t *testing.T) {
	tools := NewTestTools(t)

	linesCount := 10
	tests.CheckNotError(t, tools.CreateFile("input", tools.GetLinesForSplitting(linesCount)))

	fatalTimer := time.AfterFunc(tools.Quantum*time.Duration(linesCount+1), func() {
		t.Fatalf("timed out")
	})
	defer fatalTimer.Stop()

	ctx, cancel := context.WithTimeout(tools.Ctx, tools.Quantum*2)
	defer cancel()

	_, err := SplitFileToSortedChunks(ctx, "input", tools.SplittingOpts, func(ctx context.Context, chunk StringsChunk, filePath string) error {
		return tools.Sleep(ctx, tools.Quantum)
	})

	tests.CheckErrorIs(t, context.DeadlineExceeded, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}
