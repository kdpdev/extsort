package misc

import (
	"sync"

	"golang.org/x/exp/constraints"
)

type ProgressConstraint interface {
	constraints.Unsigned
}

func NewSafeProgress[T ProgressConstraint](max T) Progress[T] {
	return NewProgress[T](max, true)
}

func NewUnsafeProgress[T ProgressConstraint](max T) Progress[T] {
	return NewProgress[T](max, false)
}

func NewProgress[T ProgressConstraint](max T, threadsafe bool) Progress[T] {
	fnGuardRw := func() func() { return func() {} }
	fnGuard := func() func() { return func() {} }
	if threadsafe {
		guard := &sync.RWMutex{}
		fnGuardRw = func() func() {
			guard.RLock()
			return guard.RUnlock
		}
		fnGuard = func() func() {
			guard.Lock()
			return guard.Unlock
		}
	}

	var val T
	var percents T

	return Progress[T]{
		max:       max,
		val:       &val,
		percents:  &percents,
		fnGuard:   fnGuard,
		fnGuardRw: fnGuardRw,
	}
}

type Progress[T ProgressConstraint] struct {
	max       T
	val       *T
	percents  *T
	fnGuard   func() func()
	fnGuardRw func() func()
}

func (this *Progress[T]) Max() T {
	defer this.fnGuardRw()()
	return this.max
}

func (this *Progress[T]) Value() T {
	defer this.fnGuardRw()()
	return *this.val
}

func (this *Progress[T]) Percents() T {
	defer this.fnGuardRw()()
	return *this.percents
}

func (this *Progress[T]) Add(val T) (percents T, value T, percentsChanged bool) {
	defer this.fnGuard()()

	val += *this.val
	if val > this.max {
		val = this.max
	}

	if this.max != 0 {
		percents = val * 100 / this.max
	} else {
		percents = 100
	}
	percentsChanged = percents != *this.percents

	*this.val = val
	*this.percents = percents

	return percents, val, percentsChanged
}

func (this *Progress[T]) Done() (percents T, value T, percentsChanged bool) {
	defer this.fnGuard()()
	return this.Add(this.max - *this.val)
}
