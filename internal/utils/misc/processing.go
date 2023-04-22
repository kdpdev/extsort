package misc

import (
	"errors"
	"runtime"
	"sync"
)

var (
	ErrProcessorClosed = errors.New("processor already closed")
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Processor interface {
	Exec(task func()) error
	Close() error
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func NewProcessorGuard(proc Processor) Processor {
	return &processorGuard{
		guard: &sync.RWMutex{},
		proc:  proc,
	}
}

func NewSyncProcessor() Processor {
	return withCloseGuard(&syncProcessor{})
}

func NewAsyncProcessor(threads int) Processor {
	if threads <= 0 {
		threads = runtime.NumCPU()
	}

	proc := &nAsyncProcessor{}
	proc.startThreads(threads)
	return withCloseGuard(proc)
}

func withCloseGuard(proc Processor) Processor {
	return &processorCloseGuard{proc: proc}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type processorCloseGuard struct {
	closed bool
	proc   Processor
}

func (this *processorCloseGuard) Exec(task func()) error {
	if this.closed {
		return ErrProcessorClosed
	}
	return this.proc.Exec(task)
}

func (this *processorCloseGuard) Close() error {
	if this.closed {
		return ErrProcessorClosed
	}

	err := this.proc.Close()
	if err != nil {
		return err
	}

	this.closed = true
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type processorGuard struct {
	guard *sync.RWMutex
	proc  Processor
}

func (this *processorGuard) Exec(task func()) error {
	this.guard.RLock()
	defer this.guard.RUnlock()
	return this.proc.Exec(task)
}

func (this *processorGuard) Close() error {
	this.guard.Lock()
	defer this.guard.Unlock()
	return this.proc.Close()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type syncProcessor struct {
}

func (_ *syncProcessor) Exec(task func()) error {
	task()
	return nil
}

func (_ *syncProcessor) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type asyncProcessor struct {
	wg sync.WaitGroup
}

func (this *asyncProcessor) Exec(task func()) error {
	this.wg.Add(1)
	go func() {
		defer this.wg.Done()
		task()
	}()
	return nil
}

func (this *asyncProcessor) Close() error {
	this.wg.Wait()
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type nAsyncProcessor struct {
	tasksChan chan func()
	wg        sync.WaitGroup
}

func (this *nAsyncProcessor) Exec(task func()) error {
	this.tasksChan <- task
	return nil
}

func (this *nAsyncProcessor) Close() error {
	SafeChanClose(this.tasksChan)
	this.wg.Wait()
	return nil
}

func (this *nAsyncProcessor) startThreads(threads int) {
	this.tasksChan = make(chan func())
	for i := 0; i < threads; i++ {
		this.wg.Add(1)
		go func() {
			defer this.wg.Done()
			for task := range this.tasksChan {
				task()
			}
		}()
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
