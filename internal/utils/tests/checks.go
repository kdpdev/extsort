package tests

import (
	"errors"
	"runtime"
	"testing"
)

func CheckExpected[T comparable](t *testing.T, expected, val T) {
	if val != expected {
		fatalCheck(t, "Unexpected = '%v'. Expected: '%v'", val, expected)
	}
}

func CheckExpectedf[T comparable](t *testing.T, expected, val T, format string, args ...interface{}) {
	if val != expected {
		fatalCheck(t, format+"\nUnexpected %v = '%v'. Expected: '%v'", append(args, val, expected)...)
	}
}

func CheckNotNilPtr[T any](t *testing.T, obj *T) {
	if obj == nil {
		fatalCheck(t, "pointer is nil")
	}
}

func CheckNotNilPtrf[T any](t *testing.T, obj *T, format string, args ...interface{}) {
	if obj == nil {
		fatalCheck(t, format+"\npointer is nil", append(args))
	}
}

func CheckNotError(t *testing.T, err error) {
	if err != nil {
		fatalCheck(t, "Error: %v", err)
	}
}

func CheckNotErrorf(t *testing.T, err error, format string, args ...interface{}) {
	if err != nil {
		fatalCheck(t, format+"\nError: %v", append(args, err)...)
	}
}

func CheckErrorIs(t *testing.T, expected, err error) {
	if !errors.Is(err, expected) {
		fatalCheck(t, "Unexpected error: %v\nExpected: %v", err, expected)
	}
}

func CheckErrorIsf(t *testing.T, expected, err error, format string, args ...interface{}) {
	if !errors.Is(err, expected) {
		fatalCheck(t, format+"\nUnexpected error: %v\nExpected: %v", append(args, err, expected)...)
	}
}

func fatalCheck(t *testing.T, format string, args ...interface{}) {
	_, file, line, _ := runtime.Caller(2)
	if len(format) > 0 && format[0] == '\n' {
		format = format[1:]
	}
	t.Fatalf("%v:%v:\n"+format, append([]interface{}{file, line}, args...)...)
}
