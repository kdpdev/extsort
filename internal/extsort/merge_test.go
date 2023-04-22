package extsort

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kdpdev/extsort/internal/utils/tests"
)

func Test_MergeFiles_1(t *testing.T) {
	tools := NewTestTools(t)

	tests.CheckErrorIs(t, os.ErrNotExist, MergeFiles(tools.Ctx, tools.MergingOpts, "left", "right", "merged"))
	tests.CheckNotError(t, tools.CreateFile("file", ""))
	tests.CheckErrorIs(t, os.ErrNotExist, MergeFiles(tools.Ctx, tools.MergingOpts, "file", "right", "merged"))
	tests.CheckNotError(t, tools.CheckAbsent("merged"))
	tests.CheckErrorIs(t, os.ErrNotExist, MergeFiles(tools.Ctx, tools.MergingOpts, "left", "file", "merged"))
	tests.CheckNotError(t, tools.CheckAbsent("merged"))
	tests.CheckErrorIs(t, os.ErrPermission, MergeFiles(tools.Ctx, tools.MergingOpts, "file", "file", "merged"))
	tests.CheckNotError(t, tools.CheckAbsent("merged"))

	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_MergeFiles_2(t *testing.T) {
	tools := NewTestTools(t)

	tests.CheckNotError(t, tools.CreateFile("left", ""))
	tests.CheckNotError(t, tools.CreateFile("right", ""))
	tests.CheckNotError(t, MergeFiles(tools.Ctx, tools.MergingOpts, "left", "right", "merged"))
	tests.CheckNotError(t, tools.CheckPresent("merged"))
	tests.CheckNotError(t, tools.CheckAbsent("left"))
	tests.CheckNotError(t, tools.CheckAbsent("right"))
	tests.CheckNotError(t, tools.CreateFile("left", ""))
	tests.CheckNotError(t, tools.CreateFile("right", ""))
	tests.CheckErrorIs(t, os.ErrExist, MergeFiles(tools.Ctx, tools.MergingOpts, "left", "right", "merged"))
	tests.CheckNotError(t, tools.CheckPresent("merged"))

	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_MergeFiles_3(t *testing.T) {
	tools := NewTestTools(t)

	lines := "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n"

	tests.CheckNotError(t, tools.CreateFile("left", lines))
	tests.CheckNotError(t, tools.CreateFile("right", ""))
	tests.CheckNotError(t, MergeFiles(tools.Ctx, tools.MergingOpts, "left", "right", "merged"))
	tests.CheckNotError(t, tools.CheckPresent("merged"))
	tests.CheckNotError(t, tools.CheckAbsent("left"))
	tests.CheckNotError(t, tools.CheckAbsent("right"))
	merged, _, err := tools.Fs.OpenReadFile("merged")
	tests.CheckNotError(t, err)
	mergedData, err := ioutil.ReadAll(merged)
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, merged.Close())
	tests.CheckExpected(t, lines, string(mergedData))

	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_MergeFiles_4(t *testing.T) {
	tools := NewTestTools(t)

	lines := "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n"

	tests.CheckNotError(t, tools.CreateFile("left", ""))
	tests.CheckNotError(t, tools.CreateFile("right", lines))
	tests.CheckNotError(t, MergeFiles(tools.Ctx, tools.MergingOpts, "left", "right", "merged"))
	tests.CheckNotError(t, tools.CheckPresent("merged"))
	tests.CheckNotError(t, tools.CheckAbsent("left"))
	tests.CheckNotError(t, tools.CheckAbsent("right"))

	merged, _, err := tools.Fs.OpenReadFile("merged")
	tests.CheckNotError(t, err)
	mergedData, err := ioutil.ReadAll(merged)
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, merged.Close())
	tests.CheckExpected(t, lines, string(mergedData))

	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_MergeFiles_5(t *testing.T) {
	tools := NewTestTools(t)

	leftLines := "0\n2\n4\n6\n8\n"
	rightLines := "1\n3\n5\n7\n9\n"
	expectedLines := "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n"

	tests.CheckNotError(t, tools.CreateFile("left", leftLines))
	tests.CheckNotError(t, tools.CreateFile("right", rightLines))
	tests.CheckNotError(t, MergeFiles(tools.Ctx, tools.MergingOpts, "left", "right", "merged"))
	tests.CheckNotError(t, tools.CheckPresent("merged"))
	tests.CheckNotError(t, tools.CheckAbsent("left"))
	tests.CheckNotError(t, tools.CheckAbsent("right"))

	merged, _, err := tools.Fs.OpenReadFile("merged")
	tests.CheckNotError(t, err)
	mergedData, err := ioutil.ReadAll(merged)
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, merged.Close())
	tests.CheckExpected(t, expectedLines, string(mergedData))

	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_MergeFiles_Cancel_1(t *testing.T) {
	tools := NewTestTools(t)

	ctx, cancel := context.WithCancel(tools.Ctx)
	cancel()

	tests.CheckErrorIs(t, context.Canceled, MergeFiles(ctx, tools.MergingOpts, "left", "right", "merged"))
	tests.CheckNotError(t, tools.CheckAbsent("merged"))

	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_MergeFiles_Cancel_2(t *testing.T) {
	tools := NewTestTools(t)

	ctx, cancel := context.WithCancel(tools.Ctx)
	cancel()

	tests.CheckNotError(t, tools.CreateFile("left", "0\n2\n4\n6\n8\n"))
	tests.CheckNotError(t, tools.CreateFile("right", "1\n3\n5\n7\n9\n"))
	tests.CheckErrorIs(t, context.Canceled, MergeFiles(ctx, tools.MergingOpts, "left", "right", "merged"))
	tests.CheckNotError(t, tools.CheckAbsent("merged"))

	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_MergeFiles_Timeout_1(t *testing.T) {
	tools := NewTestTools(t)

	ctx, cancel := context.WithTimeout(tools.Ctx, tools.Quantum*2)
	defer cancel()
	<-ctx.Done()

	tests.CheckErrorIs(t, context.DeadlineExceeded, MergeFiles(ctx, tools.MergingOpts, "left", "right", "merged"))
	tests.CheckNotError(t, tools.CheckAbsent("merged"))

	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_MergeFiles_Timeout_2(t *testing.T) {
	tools := NewTestTools(t)

	ctx, cancel := context.WithTimeout(tools.Ctx, tools.Quantum*2)
	defer cancel()
	<-ctx.Done()

	tests.CheckNotError(t, tools.CreateFile("left", "0\n2\n4\n6\n8\n"))
	tests.CheckNotError(t, tools.CreateFile("right", "1\n3\n5\n7\n9\n"))
	tests.CheckErrorIs(t, context.DeadlineExceeded, MergeFiles(ctx, tools.MergingOpts, "left", "right", "merged"))
	tests.CheckNotError(t, tools.CheckAbsent("merged"))

	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_Merge_1(t *testing.T) {
	tools := NewTestTools(t)
	_, err := Merge(tools.Ctx, nil, tools.MergingOpts, nil)
	tests.CheckErrorIs(t, ErrNoFiles, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_Merge_2(t *testing.T) {
	tools := NewTestTools(t)

	tests.CheckNotError(t, tools.CreateFile("file", ""))
	merged, err := Merge(tools.Ctx, []string{"file"}, tools.MergingOpts, nil)
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, tools.CheckAbsent("file"))
	tests.CheckNotError(t, tools.CheckPresent(merged))
	dir, _ := filepath.Split(merged)
	tests.CheckExpected(t, tools.MergingOpts.OutputDir, strings.TrimRight(dir, string(filepath.Separator)))
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_Merge_3(t *testing.T) {
	tools := NewTestTools(t)

	tests.CheckNotError(t, tools.CreateFile("file1", ""))
	tests.CheckNotError(t, tools.CreateFile("file2", ""))
	merged, err := Merge(tools.Ctx, []string{"file1", "file2"}, tools.MergingOpts, nil)
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, tools.CheckAbsent("file1"))
	tests.CheckNotError(t, tools.CheckAbsent("file2"))
	tests.CheckNotError(t, tools.CheckPresent(merged))
	dir, _ := filepath.Split(merged)
	tests.CheckExpected(t, tools.MergingOpts.OutputDir, strings.TrimRight(dir, string(filepath.Separator)))
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_Merge_4(t *testing.T) {
	tools := NewTestTools(t)

	lines1 := "x\ny\nz\n"
	lines2 := "a\nb\nc\nd\ne\nf\n"
	lines3 := "1\n2\n3\n4\n"

	tests.CheckNotError(t, tools.CreateFile("file1", lines1))
	tests.CheckNotError(t, tools.CreateFile("file2", lines2))
	tests.CheckNotError(t, tools.CreateFile("file3", lines3))
	mergedPath, err := Merge(tools.Ctx, []string{"file1", "file2", "file3"}, tools.MergingOpts, nil)
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, tools.CheckAbsent("file1"))
	tests.CheckNotError(t, tools.CheckAbsent("file2"))
	tests.CheckNotError(t, tools.CheckAbsent("file3"))
	tests.CheckNotError(t, tools.CheckPresent(mergedPath))
	dir, _ := filepath.Split(mergedPath)
	tests.CheckExpected(t, tools.MergingOpts.OutputDir, strings.TrimRight(dir, string(filepath.Separator)))

	mergedFile, _, err := tools.Fs.OpenReadFile(mergedPath)
	tests.CheckNotError(t, err)

	mergedData, err := ioutil.ReadAll(mergedFile)
	tests.CheckNotError(t, err)
	tests.CheckNotError(t, mergedFile.Close())

	mergedStr := string(mergedData)
	tests.CheckExpected(t, strings.ReplaceAll(lines3+lines2+lines1, "\n", "_"), strings.ReplaceAll(mergedStr, "\n", "_"))

	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_Merge_Cancel_1(t *testing.T) {
	tools := NewTestTools(t)

	ctx, cancel := context.WithCancel(tools.Ctx)
	cancel()

	_, err := Merge(ctx, nil, tools.MergingOpts, func(ctx context.Context, left, right, out string) error {
		return fmt.Errorf("unexpected")
	})

	tests.CheckErrorIs(t, context.Canceled, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_Merge_Cancel_2(t *testing.T) {
	tools := NewTestTools(t)

	ctx, cancel := context.WithCancel(tools.Ctx)
	cancel()

	_, err := Merge(ctx, []string{"file1", "file2", "file3"}, tools.MergingOpts, func(ctx context.Context, left, right, out string) error {
		return fmt.Errorf("unexpected")
	})

	tests.CheckErrorIs(t, context.Canceled, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_Merge_Cancel_3(t *testing.T) {
	tools := NewTestTools(t)

	files := make([]string, 0)
	for i := 0; i < 20; i++ {
		file := fmt.Sprintf("file_%v", i)
		files = append(files, file)
		tests.CheckNotError(t, tools.CreateFile(file, ""))
	}

	fatalTimer := time.AfterFunc(tools.Quantum*time.Duration(10), func() {
		t.Fatalf("timed out")
	})
	defer fatalTimer.Stop()

	ctx, cancel := context.WithCancel(tools.Ctx)
	defer cancel()
	cancelTimer := time.AfterFunc(tools.Quantum*2, cancel)
	defer cancelTimer.Stop()

	tools.MergingOpts.WorkersCount = 1
	_, err := Merge(ctx, files, tools.MergingOpts, func(ctx context.Context, left, right, out string) error {
		return tools.Sleep(ctx, tools.Quantum)
	})

	tests.CheckErrorIs(t, context.Canceled, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_Merge_Timeout_1(t *testing.T) {
	tools := NewTestTools(t)

	ctx, cancel := context.WithTimeout(tools.Ctx, tools.Quantum*2)
	defer cancel()
	<-ctx.Done()

	_, err := Merge(ctx, nil, tools.MergingOpts, func(ctx context.Context, left, right, out string) error {
		return fmt.Errorf("unexpected")
	})

	tests.CheckErrorIs(t, context.DeadlineExceeded, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_Merge_Timeout_2(t *testing.T) {
	tools := NewTestTools(t)

	ctx, cancel := context.WithTimeout(tools.Ctx, tools.Quantum*2)
	defer cancel()
	<-ctx.Done()

	_, err := Merge(ctx, []string{"file1", "file2", "file3"}, tools.MergingOpts, func(ctx context.Context, left, right, out string) error {
		return fmt.Errorf("unexpected")
	})

	tests.CheckErrorIs(t, context.DeadlineExceeded, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}

func Test_Merge_Timeout_3(t *testing.T) {
	tools := NewTestTools(t)

	files := make([]string, 0)
	for i := 0; i < 20; i++ {
		file := fmt.Sprintf("file_%v", i)
		files = append(files, file)
		tests.CheckNotError(t, tools.CreateFile(file, ""))
	}

	fatalTimer := time.AfterFunc(tools.Quantum*time.Duration(10), func() {
		t.Fatalf("timed out")
	})
	defer fatalTimer.Stop()

	ctx, cancel := context.WithTimeout(tools.Ctx, tools.Quantum*2)
	defer cancel()

	tools.MergingOpts.WorkersCount = 1
	_, err := Merge(ctx, files, tools.MergingOpts, func(ctx context.Context, left, right, out string) error {
		return tools.Sleep(ctx, tools.Quantum)
	})

	tests.CheckErrorIs(t, context.DeadlineExceeded, err)
	tests.CheckExpected(t, 0, len(tools.UnhandledErrs()))
	tests.CheckExpected(t, false, tools.Fs.HasOpenedEntries())
}
