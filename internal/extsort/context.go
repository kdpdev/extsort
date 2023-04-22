package extsort

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/kdpdev/extsort/internal/extsort/env"
)

type contextKeyType int

var (
	contextKeyScope                   = contextKeyType(1)
	contextKeyLogger                  = contextKeyType(2)
	contextKeyFs                      = contextKeyType(3)
	contextKeyUnhandledErrorHandler   = contextKeyType(4)
	contextKeyUnhandledErrorDecorator = contextKeyType(5)
)

type Logf = func(format string, args ...interface{})
type UnhandledErrorHandler = func(ctx context.Context, err error)
type UnhandledErrorDecorator = func(ctx context.Context, err error) error

func DefaultLogf(format string, args ...interface{}) {
	if len(format) > 0 && format[len(format)-1] != '\n' {
		format += "\n"
	}
	fmt.Printf(format, args...)
}

func PrefixedLogger(prefix string, logf Logf) Logf {
	prefix += ": "
	return func(format string, args ...interface{}) {
		logf("%s", prefix+fmt.Sprintf(format, args...))
	}
}

func NoLogf(format string, args ...interface{}) {
}

func getContextValue[T any](ctx context.Context, key contextKeyType, defaultValue T) T {
	val := ctx.Value(key)
	if val == nil {
		return defaultValue
	}
	typedVal, ok := val.(T)
	if !ok {
		return defaultValue
	}
	return typedVal
}

func GetLogger(ctx context.Context) Logf {
	return getContextValue(ctx, contextKeyLogger, NoLogf)
}

func WithLogger(ctx context.Context, logf Logf) context.Context {
	return context.WithValue(ctx, contextKeyLogger, logf)
}

func WithPrefixedLogger(ctx context.Context, prefix string) (context.Context, Logf) {
	prefixedLogf := PrefixedLogger(prefix, GetLogger(ctx))
	return WithLogger(ctx, prefixedLogf), prefixedLogf
}

func GetFs(ctx context.Context) env.Fs {
	return getContextValue(ctx, contextKeyFs, env.NewMockFs())
}

func WithFs(ctx context.Context, fs env.Fs) context.Context {
	return context.WithValue(ctx, contextKeyFs, fs)
}

func GetScope(ctx context.Context) string {
	return getContextValue(ctx, contextKeyScope, "")
}

func WithScope(ctx context.Context, scope string) context.Context {
	return context.WithValue(ctx, contextKeyScope, fmt.Sprintf("%v%v: ", GetScope(ctx), scope))
}

func WithCallerScope(ctx context.Context) context.Context {
	pc, _, _, _ := runtime.Caller(1)
	return WithScope(ctx, filepath.Base(runtime.FuncForPC(pc).Name()))
}

func GetUnhandledErrorDecorator(ctx context.Context) UnhandledErrorDecorator {
	return getContextValue(ctx, contextKeyUnhandledErrorDecorator, NoErrorDecorator())
}

func WithUnhandledErrorDecorator(ctx context.Context, decorators ...UnhandledErrorDecorator) context.Context {
	return context.WithValue(ctx, contextKeyUnhandledErrorDecorator, ComposeUnhandledErrorDecorators(decorators...))
}

func getUnhandledErrorHandlerWithoutErrorDecorating(ctx context.Context) UnhandledErrorHandler {
	return getContextValue(ctx, contextKeyUnhandledErrorHandler, NoErrHandler())
}

func GetUnhandledErrorHandler(ctx context.Context) UnhandledErrorHandler {
	h := getContextValue(ctx, contextKeyUnhandledErrorHandler, NoErrHandler())
	return func(ctx context.Context, err error) {
		decorate := GetUnhandledErrorDecorator(ctx)
		h(ctx, decorate(ctx, err))
	}
}

func GetContextedUnhandledErrorHandler(ctx context.Context) func(err error) {
	h := getContextValue(ctx, contextKeyUnhandledErrorHandler, NoErrHandler())
	return func(err error) {
		decorate := GetUnhandledErrorDecorator(ctx)
		h(ctx, decorate(ctx, err))
	}
}

func WithUnhandledErrorHandler(ctx context.Context, h UnhandledErrorHandler) context.Context {
	return context.WithValue(ctx, contextKeyUnhandledErrorHandler, h)
}

func WithUnhandledErrorLogger(ctx context.Context) context.Context {
	return WithUnhandledErrorHandler(ctx, func(ctx context.Context, err error) {
		GetLogger(ctx)("%v", err)
	})
}

func WithUnhandledErrorFilter(ctx context.Context, pass func(err error) bool) context.Context {
	prev := getUnhandledErrorHandlerWithoutErrorDecorating(ctx)
	return WithUnhandledErrorHandler(ctx, func(ctx context.Context, err error) {
		if pass(err) {
			prev(ctx, err)
		}
	})
}

func WithUnhandledErrorContextErrorsFilter(ctx context.Context) context.Context {
	skips := []error{context.Canceled, context.DeadlineExceeded}
	return WithUnhandledErrorFilter(ctx, func(err error) bool {
		for _, skippingError := range skips {
			if errors.Is(err, skippingError) {
				return false
			}
		}
		return true
	})
}

func WithUnhandledErrorsCollector(ctx context.Context) (context.Context, func() []error) {
	guard := &sync.Mutex{}
	errs := make([]error, 0)
	getErrors := func() []error { return errs }
	prev := getUnhandledErrorHandlerWithoutErrorDecorating(ctx)
	return WithUnhandledErrorHandler(ctx, func(ctx context.Context, err error) {
		guard.Lock()
		defer guard.Unlock()
		errs = append(errs, err)
		prev(ctx, err)
	}), getErrors
}

func OnUnhandledError(ctx context.Context, err error) {
	GetContextedUnhandledErrorHandler(ctx)(err)
}

func NoErrHandler() UnhandledErrorHandler {
	return func(ctx context.Context, err error) {
	}
}

func NoErrorDecorator() UnhandledErrorDecorator {
	return func(ctx context.Context, err error) error {
		return err
	}
}

func DefaultUnhandledErrorDecorator() UnhandledErrorDecorator {
	return func(ctx context.Context, err error) error {
		return fmt.Errorf("UNHANDLED ERROR: %v%w", GetScope(ctx), err)
	}
}

func ComposeUnhandledErrorDecorators(decorators ...UnhandledErrorDecorator) UnhandledErrorDecorator {
	return func(ctx context.Context, err error) error {
		for _, d := range decorators {
			err = d(ctx, err)
		}
		return err
	}
}
