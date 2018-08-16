package util

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}

func WithRecover(fn func(), panicHandler func(error)) {
	defer func() {
		if err := recover(); err != nil {
			if _, ok := err.(error); ok {
				panicHandler(err.(error))
			}
		}
	}()

	fn()
}
