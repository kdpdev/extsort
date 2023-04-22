package misc

func InvokeIfFalse(flag *bool, fn func()) {
	InvokeIfEq(flag, false, fn)
}

func InvokeIfTrue(flag *bool, fn func()) {
	InvokeIfEq(flag, true, fn)
}

func InvokeIfError(err *error, fn func()) {
	if fn != nil && err != nil && *err != nil {
		fn()
	}
}

func InvokeIfNotError(err *error, fn func()) {
	if fn != nil && err != nil && *err == nil {
		fn()
	}
}

func InvokeIfEq[T comparable](valPtr *T, val T, fn func()) {
	if fn != nil && valPtr != nil && *valPtr == val {
		fn()
	}
}

func InvokeIfNotEq[T comparable](valPtr *T, val T, fn func()) {
	if fn != nil && valPtr != nil && *valPtr != val {
		fn()
	}
}
