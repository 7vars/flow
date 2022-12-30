package flow

import (
	"context"
	"fmt"
	"sync"
)

// ===== handler =====

type SinkHandler interface {
	OnPush(Inlet, any)
	OnError(Inlet, error)
	OnComplete()
}

type Sink struct {
	DoPush     func(Inlet, any)
	DoError    func(Inlet, error)
	DoComplete func()
}

func (sink Sink) OnPush(in Inlet, v any) {
	if sink.DoPush != nil {
		sink.DoPush(in, v)
		return
	}
	in.Pull()
}

func (sink Sink) OnError(in Inlet, e error) {
	if sink.DoError != nil {
		sink.DoError(in, e)
		return
	}
	in.Cancel()
}

func (sink Sink) OnComplete() {
	if sink.DoComplete != nil {
		sink.DoComplete()
	}
}

type SinkHandlerFunc[T any] func(T) error

func (shf SinkHandlerFunc[T]) OnPush(in Inlet, v any) {
	if t, ok := v.(T); ok {
		if err := shf(t); err != nil {
			shf.OnError(in, err)
			return
		}
		in.Pull()
		return
	}
	var t0 T
	shf.OnError(in, fmt.Errorf("sink: expect type %T got %T", t0, v))
}

func (shf SinkHandlerFunc[T]) OnError(in Inlet, e error) {
	in.Cancel()
}

func (shf SinkHandlerFunc[T]) OnComplete() {}

// ===== builder =====

type SinkBuilder interface {
	Build() SinkHandler
}

type SinkBuilderFunc func() SinkHandler

func (sbf SinkBuilderFunc) Build() SinkHandler {
	return sbf()
}

// ===== process =====

type SinkProcess func(context.Context, ...Inline)

func (sp SinkProcess) Process(ctx context.Context, inlines []Inline, outlines []Outline) {
	if len(outlines) > 0 {
		panic("sink: outlines not supported")
	}
	sp(ctx, inlines...)
}

// ===== sinks =====

func SinkProcessor(builder SinkBuilder) Processor {
	return SinkProcess(func(ctx context.Context, inlines ...Inline) {
		kill := make(chan struct{}, 1)
		var wg sync.WaitGroup
		wg.Add(len(inlines))
		cancelWorker := make([]chan<- error, len(inlines))

		for i, inline := range inlines {
			cancel := make(chan error, 1)
			cancelWorker[i] = cancel
			go func(grp *sync.WaitGroup, in Inline, cancel <-chan error, handler SinkHandler) {
				defer grp.Done()
				var err error
				for {
					select {
					case evt, open := <-in.Events():
						if !open {
							return
						}
						switch evt.Type() {
						case PUSH:
							if err != nil {
								handler.OnError(in, err)
								continue
							}
							handler.OnPush(in, evt.Data)
						case ERROR:
							handler.OnError(in, evt.Error)
						case COMPLETE:
							handler.OnComplete()
							return
						}
					case e := <-cancel:
						if e != nil {
							err = e
							continue
						}
					}
				}
			}(&wg, inline, cancel, builder.Build())
		}

		go func(cancels ...chan<- error) {
			for {
				select {
				case <-kill:
					return
				case <-ctx.Done():
					err := ctx.Err()
					for _, cls := range cancels {
						defer close(cls)
						cls <- err
					}
				}
			}
		}(cancelWorker...)

		go func() {
			defer close(kill)
			wg.Wait()
			kill <- struct{}{}
		}()
	})
}
