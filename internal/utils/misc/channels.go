package misc

import "sync"

func SafeChanClose[T any](ch chan T) {
	select {
	case _, ok := <-ch:
		if ok {
			close(ch)
		}
	default:
		close(ch)
	}
}

func MergeChannels[T any](chans ...<-chan T) <-chan T {
	result := make(chan T)
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch <-chan T) {
			defer wg.Done()
			for obj := range ch {
				result <- obj
			}
		}(ch)
	}

	go func() {
		defer close(result)
		wg.Wait()
	}()

	return result
}
