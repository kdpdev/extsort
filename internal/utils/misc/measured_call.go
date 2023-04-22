package misc

import "time"

func MeasureCall(fn func()) time.Duration {
	begin := time.Now().UnixMicro()
	fn()
	end := time.Now().UnixMicro()
	dur := time.Duration(end-begin) * time.Microsecond
	return dur
}

func MeasureCallE(fn func() error) (dur time.Duration, err error) {
	dur = MeasureCall(func() {
		err = fn()
	})
	return
}

func MeasureCallRE[Result any](fn func() (Result, error)) (dur time.Duration, res Result, err error) {
	dur = MeasureCall(func() {
		res, err = fn()
	})
	return
}
