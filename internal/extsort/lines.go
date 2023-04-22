package extsort

import (
	"bufio"
	"bytes"
	"context"
	"io"
)

type LinesGen func() (line string, done bool, err error)

func NewSyncLinesGenFromReader(ctx context.Context, reader io.Reader) LinesGen {
	scanner := newLinesScanner(reader)
	return func() (string, bool, error) {
		if err := ctx.Err(); err != nil {
			return "", true, err
		}
		if scanner.Scan() {
			return scanner.Text(), false, nil
		}
		return "", true, scanner.Err()
	}
}

func NewAsyncLinesFromReader(ctx context.Context, reader io.Reader) LinesGen {
	linesChan, linesErr := NewLinesChan(ctx, reader)
	return func() (string, bool, error) {
		line, ok := <-linesChan
		if !ok {
			return line, true, linesErr()
		}
		return line, false, nil
	}
}

func EnumLines(nextLine LinesGen, consume func(line string) error) (int, error) {
	linesCount := 0
	done := false
	var err error
	line := ""
	for !done && err == nil {
		line, done, err = nextLine()
		if !done {
			linesCount++
			err = consume(line)
		}
	}
	return linesCount, err
}

func CollectLines(nextLine LinesGen) ([]string, error) {
	lines := make([]string, 0)
	_, err := EnumLines(nextLine, func(line string) error {
		lines = append(lines, line)
		return nil
	})
	return lines, err
}

func NewLinesChan(ctx context.Context, reader io.Reader) (<-chan string, func() error) {
	linesChan := make(chan string)
	var err error

	go func() {
		defer close(linesChan)

		scanner := newLinesScanner(reader)

		for scanner.Scan() {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return

			case linesChan <- scanner.Text():
				continue
			}
		}

		err = scanner.Err()
	}()

	return linesChan, func() error { return err }
}

func newLinesScanner(reader io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(reader)
	scanner.Split(scanLines)
	return scanner
}

func scanLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[0:i], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}
