package misc

import "sync"

type OnceError interface {
	Get() error
	TrySet(err error) bool
	Invoke(fn func() error)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func NewOnceError(err *error) OnceError {
	if err == nil {
		err = new(error)
	}
	return &onceError{err: err}
}

func NewOnceEventWithNotSetNotification(err OnceError, onNotSet func(error)) OnceError {
	return &onceErrorWithNotSetNotification{
		err:      err,
		onNotSet: onNotSet,
	}
}

func NewOnceEventWithGuard(err OnceError, guard *sync.RWMutex) OnceError {
	if guard == nil {
		guard = &sync.RWMutex{}
	}
	return &onceErrorWithGuard{
		err:   err,
		guard: guard,
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type onceError struct {
	err *error
}

func (this *onceError) TrySet(err error) bool {
	if err == nil {
		return false
	}

	if *this.err != nil {
		return false
	}

	*this.err = err
	return true
}

func (this *onceError) Get() error {
	if this.err == nil {
		return nil
	}
	return *this.err
}

func (this *onceError) Invoke(fn func() error) {
	this.TrySet(fn())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type onceErrorWithNotSetNotification struct {
	err      OnceError
	onNotSet func(err error)
}

func (this *onceErrorWithNotSetNotification) TrySet(err error) bool {
	if err == nil {
		return false
	}

	if !this.err.TrySet(err) {
		this.onNotSet(err)
		return false
	}

	return true
}

func (this *onceErrorWithNotSetNotification) Get() error {
	return this.err.Get()
}

func (this *onceErrorWithNotSetNotification) Invoke(fn func() error) {
	this.TrySet(fn())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type onceErrorWithGuard struct {
	err   OnceError
	guard *sync.RWMutex
}

func (this *onceErrorWithGuard) TrySet(err error) bool {
	if err == nil {
		return false
	}

	this.guard.Lock()
	defer this.guard.Unlock()
	return this.err.TrySet(err)
}

func (this *onceErrorWithGuard) Get() error {
	this.guard.RLock()
	defer this.guard.RUnlock()
	return this.err.Get()
}

func (this *onceErrorWithGuard) Invoke(fn func() error) {
	this.TrySet(fn())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
