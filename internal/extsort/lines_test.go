package extsort

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kdpdev/extsort/internal/utils/tests"
)

func getAllLinesGenCreators() map[string]func(ctx context.Context, reader io.Reader) LinesGen {
	return map[string]func(ctx context.Context, reader io.Reader) LinesGen{
		" sync": NewSyncLinesGenFromReader,
		"async": NewAsyncLinesFromReader,
	}
}

func Test_LinesReading(t *testing.T) {

	formatLinesStringForLog := func(str string) string {
		str = strings.ReplaceAll(str, "\r", "\\r")
		str = strings.ReplaceAll(str, "\n", "\\n ")
		return str
	}

	joinLinesWithEol := func(lines []string) string {
		joined := ""
		for _, l := range lines {
			joined += l + "\n"
		}
		return joined
	}

	joinLinesWithoutEol := func(lines []string) string {
		joined := ""
		for _, l := range lines {
			joined += l + "\n"
		}
		if len(lines) != 0 && lines[len(lines)-1] != "" {
			joined = joined[:len(joined)-1]
		}
		return joined
	}

	testLinesReading := func(
		logf Logf,
		lines []string,
		joinLines func(lines []string) string,
		create func(ctx context.Context, reader io.Reader) LinesGen) error {

		ctx := context.Background()

		linesStr := joinLines(lines)
		linesGen := create(ctx, strings.NewReader(linesStr))
		linesResult, err := CollectLines(linesGen)
		if err != nil {
			return err
		}

		linesStrResult := joinLines(linesResult)

		if logf != nil {
			logf("expected: [%2v](%v) result: [%2v](%v)",
				len(lines), formatLinesStringForLog(linesStr),
				len(linesResult), formatLinesStringForLog(linesStrResult))
		}

		if linesStrResult != linesStr {
			return fmt.Errorf("result of lines reading is unexpected")
		}

		return nil
	}

	cases := make([][]string, 0)
	addCr := func(strs []string) []string {
		result := make([]string, len(strs))
		for i, s := range strs {
			result[i] = s + "\r"
		}
		return result
	}
	addCase := func(lines ...string) {
		cases = append(cases, lines)
		cases = append(cases, addCr(lines))
	}

	addCase()
	addCase("")
	addCase("", "")
	addCase("", "", "")
	addCase("1")
	addCase("1", "22")
	addCase("1", "22", "333")

	makeLogFn := func(prefix string) Logf {
		return func(format string, args ...interface{}) {
			//t.Logf("%s: %s", prefix, fmt.Sprintf(format, args...))
		}
	}

	for kind, creator := range getAllLinesGenCreators() {
		for i, lines := range cases {

			var err error

			prefix1 := fmt.Sprintf("   linesWithEol: %v[%2v]", kind, i)
			logf1 := makeLogFn(prefix1)
			err = testLinesReading(logf1, lines, joinLinesWithEol, creator)
			tests.CheckNotErrorf(t, err, prefix1+" failed")

			prefix2 := fmt.Sprintf("linesWithoutEol: %v[%2v]", kind, i)
			logf2 := makeLogFn(prefix2)
			err = testLinesReading(logf2, lines, joinLinesWithoutEol, creator)
			tests.CheckNotErrorf(t, err, prefix2+" failed")
		}
	}
}

func Test_ReadLines_Cancel_1(t *testing.T) {
	test := func(logPrefix string, create func(ctx context.Context, reader io.Reader) LinesGen) error {
		linesCount := 4
		lines := ""
		for i := 0; i < linesCount; i++ {
			lines += strconv.Itoa(i) + "\n"
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		linesGen := NewSyncLinesGenFromReader(ctx, strings.NewReader(lines))
		_, err := EnumLines(linesGen, func(line string) error {
			return fmt.Errorf("unexpected")
		})

		if !errors.Is(err, context.Canceled) {
			return fmt.Errorf("unexpected error: %w", err)
		}

		return nil
	}

	for kind, creator := range getAllLinesGenCreators() {
		err := test(kind, creator)
		tests.CheckNotErrorf(t, err, "cancel of '%v' lines gen failed", kind)
	}
}

func Test_ReadLines_Cancel_2(t *testing.T) {
	test := func(logPrefix string, create func(ctx context.Context, reader io.Reader) LinesGen) error {
		linesCount := 4
		lines := ""
		for i := 0; i < linesCount; i++ {
			lines += strconv.Itoa(i) + "\n"
		}

		quantum := 50 * time.Millisecond

		fatalTimer := time.AfterFunc(quantum*time.Duration(linesCount+1), func() {
			t.Fatalf("%v: timed out", logPrefix)
		})
		defer fatalTimer.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cancelTimer := time.AfterFunc(quantum*2, cancel)
		defer cancelTimer.Stop()

		linesGen := NewSyncLinesGenFromReader(ctx, strings.NewReader(lines))
		_, err := EnumLines(linesGen, func(line string) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(quantum):
				return nil
			}
		})

		if !errors.Is(err, context.Canceled) {
			return fmt.Errorf("unexpected result: %w", err)
		}

		return nil
	}

	for kind, creator := range getAllLinesGenCreators() {
		err := test(kind, creator)
		tests.CheckNotErrorf(t, err, "cancel of '%v' lines gen failed", kind)
	}
}

func Test_ReadLines_Timeout_1(t *testing.T) {
	test := func(logPrefix string, create func(ctx context.Context, reader io.Reader) LinesGen) error {
		linesCount := 4
		lines := ""
		for i := 0; i < linesCount; i++ {
			lines += strconv.Itoa(i) + "\n"
		}

		quantum := 50 * time.Millisecond

		ctx, cancel := context.WithTimeout(context.Background(), quantum)
		defer cancel()
		<-ctx.Done()

		linesGen := NewSyncLinesGenFromReader(ctx, strings.NewReader(lines))
		_, err := EnumLines(linesGen, func(line string) error {
			return fmt.Errorf("unexpected")
		})

		if !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("unexpected result: %w", err)
		}

		return nil
	}

	for kind, creator := range getAllLinesGenCreators() {
		err := test(kind, creator)
		tests.CheckNotErrorf(t, err, "timeout of '%v' lines gen failed", kind)
	}
}

func Test_ReadLines_Timeout(t *testing.T) {
	test := func(logPrefix string, create func(ctx context.Context, reader io.Reader) LinesGen) error {
		linesCount := 4
		lines := ""
		for i := 0; i < linesCount; i++ {
			lines += strconv.Itoa(i) + "\n"
		}

		quantum := 50 * time.Millisecond

		fatalTimer := time.AfterFunc(quantum*time.Duration(linesCount+1), func() {
			t.Fatalf("%v: timed out", logPrefix)
		})
		defer fatalTimer.Stop()

		ctx, cancel := context.WithTimeout(context.Background(), quantum*2)
		defer cancel()

		linesGen := NewSyncLinesGenFromReader(ctx, strings.NewReader(lines))
		_, err := EnumLines(linesGen, func(line string) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(quantum):
				return nil
			}
		})

		if !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("unexpected result: %w", err)
		}

		return nil
	}

	for kind, creator := range getAllLinesGenCreators() {
		err := test(kind, creator)
		tests.CheckNotErrorf(t, err, "timeout of '%v' lines gen failed", kind)
	}
}
