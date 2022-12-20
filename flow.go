package flow

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

//lint:ignore ST1012 ingnore this warning
var Next = errors.New("next")

// ===== handler =====

type FlowHandler interface {
	OnPull(IOlet)
	OnCancel(IOlet)
	OnPush(IOlet, any)
	OnError(IOlet, error)
	OnComplete(Outlet)
}

type Flow struct {
	Pull     func(IOlet)
	Cancel   func(IOlet)
	Push     func(IOlet, any)
	Error    func(IOlet, error)
	Complete func(Outlet)
}

func (f Flow) OnPull(io IOlet) {
	if f.Pull != nil {
		f.Pull(io)
		return
	}
	io.Pull()
}

func (f Flow) OnCancel(io IOlet) {
	if f.Cancel != nil {
		f.Cancel(io)
		return
	}
	io.Cancel()
}

func (f Flow) OnPush(io IOlet, v any) {
	if f.Push != nil {
		f.Push(io, v)
		return
	}
	io.Push(v)
}

func (f Flow) OnError(io IOlet, e error) {
	if f.Error != nil {
		f.Error(io, e)
		return
	}
	io.Error(e)
}

func (f Flow) OnComplete(o Outlet) {
	if f.Complete != nil {
		f.Complete(o)
		return
	}
	o.Complete()
}

type FlowHandlerFunc[T, K any] func(T) (K, error)

func (FlowHandlerFunc[T, K]) OnPull(iolet IOlet) {
	iolet.Pull()
}

func (FlowHandlerFunc[T, K]) OnCancel(iolet IOlet) {
	iolet.Cancel()
}

func (fhf FlowHandlerFunc[T, K]) OnPush(iolet IOlet, v any) {
	if t, ok := v.(T); ok {
		k, err := fhf(t)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fhf.OnCancel(iolet)
				return
			}
			if errors.Is(err, Next) {
				iolet.Pull()
				return
			}
			fhf.OnError(iolet, err)
			return
		}
		iolet.Push(k)
		return
	}
	var t0 T
	fhf.OnError(iolet, fmt.Errorf("flow: expect %T got %T", t0, v))
}

func (FlowHandlerFunc[T, K]) OnError(iolet IOlet, e error) {
	iolet.Error(e)
}

func (FlowHandlerFunc[T, K]) OnComplete(outlet Outlet) {
	outlet.Complete()
}

// ===== builder =====

type FlowBuilder interface {
	Build(Inline) Inline
}

type FlowBuilderFunc func(Inline) Inline

func (fbf FlowBuilderFunc) Build(in Inline) Inline {
	return fbf(in)
}

type BuildFlow func() FlowHandler

func (bf BuildFlow) Build(in Inline) Inline {
	commands := make(chan Command)
	events := make(chan Event)

	go func(commands <-chan Command, events <-chan Event, iol IOlet, handler FlowHandler) {
		// defer fmt.Println("DEBUG: flow closed")
		var commandsClosed, eventsClosed bool
		for {
			select {
			case cmd, open := <-commands:
				if !open {
					commandsClosed = true
					if eventsClosed {
						return
					}
					continue
				}
				switch cmd {
				case PULL:
					handler.OnPull(iol)
				case CANCEL:
					handler.OnCancel(iol)
				}
			case evt, open := <-events:
				if !open {
					eventsClosed = true
					if commandsClosed {
						return
					}
					continue
				}
				switch evt.Type() {
				case PUSH:
					handler.OnPush(iol, evt.Data)
				case ERROR:
					handler.OnError(iol, evt.Error)
				case COMPLETE:
					handler.OnComplete(iol)
				}
			}
		}
	}(commands, in.Events(), iolet{in, OutletChan(events)}, bf())

	return inline{InletChan(commands), func() {
		close(commands)
		close(events)
		in.Close()
	}, events}
}

// ===== graph =====

type FlowGraph interface {
	FlowBuilder
	Via(FlowBuilder) FlowGraph
	To(SinkBuilder) SinkBuilder
}

type FlowGraphFunc func() FlowBuilder

func (fgf FlowGraphFunc) Build(in Inline) Inline {
	return fgf().Build(in)
}

func (fgf FlowGraphFunc) From(source SourceBuilder) Graph {
	return SourceGraphFunc(func() SourceBuilder {
		return SourceBuilderFunc(func() Inline {
			return fgf.Build(source.Build())
		})
	})
}

func (fgf FlowGraphFunc) Via(flow FlowBuilder) FlowGraph {
	return FlowGraphFunc(func() FlowBuilder {
		return FlowBuilderFunc(func(in Inline) Inline {
			return flow.Build(fgf().Build(in))
		})
	})
}

func (fgf FlowGraphFunc) To(sink SinkBuilder) SinkBuilder {
	return SinkBuilderFunc(func(in Inline) Runnable {
		return sink.Build(fgf.Build(in))
	})
}

// ===== flows =====

func FlowFunc[T, K any](f func(T) (K, error)) FlowGraph {
	return FlowGraphFunc(func() FlowBuilder {
		return BuildFlow(func() FlowHandler {
			return FlowHandlerFunc[T, K](f)
		})
	})
}

func Map[T, K any](f func(T) K) FlowGraph {
	return FlowFunc(func(t T) (K, error) {
		return f(t), nil
	})
}

func Filter[T any](f func(T) bool) FlowGraph {
	return FlowFunc(func(t T) (T, error) {
		if f(t) {
			return t, nil
		}
		var t0 T
		return t0, Next
	})
}

func Take(n uint64) FlowGraph {
	return FlowGraphFunc(func() FlowBuilder {
		return BuildFlow(func() FlowHandler {
			var count uint64
			return FlowHandlerFunc[any, any](func(v any) (any, error) {
				defer atomic.AddUint64(&count, 1)
				if atomic.LoadUint64(&count) < n {
					return v, nil
				}
				return v, io.EOF
			})
		})
	})
}

func TakeWhile[T any](f func(T) bool) FlowGraph {
	return FlowGraphFunc(func() FlowBuilder {
		return BuildFlow(func() FlowHandler {
			return FlowHandlerFunc[T, T](func(t T) (T, error) {
				if f(t) {
					return t, nil
				}
				var t0 T
				return t0, io.EOF
			})
		})
	})
}

func Drop(n uint64) FlowGraph {
	return FlowGraphFunc(func() FlowBuilder {
		return BuildFlow(func() FlowHandler {
			var count uint64
			return FlowHandlerFunc[any, any](func(v any) (any, error) {
				defer atomic.AddUint64(&count, 1)
				if atomic.LoadUint64(&count) < n {
					return v, Next
				}
				return v, nil
			})
		})
	})
}

func DropWhile[T any](f func(T) bool) FlowGraph {
	return FlowGraphFunc(func() FlowBuilder {
		return BuildFlow(func() FlowHandler {
			var mutex sync.Mutex
			var dropped bool
			return FlowHandlerFunc[T, T](func(t T) (T, error) {
				mutex.Lock()
				defer mutex.Unlock()
				if f(t) && !dropped {
					return t, Next
				}
				dropped = true
				return t, nil
			})
		})
	})
}

func Fold[T, K any](k K, f func(K, T) K) FlowGraph {
	return FlowGraphFunc(func() FlowBuilder {
		return BuildFlow(func() FlowHandler {
			var m sync.Mutex
			x := k
			var complete bool
			return Flow{
				Push: func(io IOlet, v any) {
					if t, ok := v.(T); ok {
						m.Lock()
						defer m.Unlock()
						x = f(x, t)
						io.Pull()
						return
					}
					var t0 T
					io.Error(fmt.Errorf("fold(%T): unsupported type %T", t0, v))
				},
				Complete: func(o Outlet) {
					m.Lock()
					defer m.Unlock()
					o.Push(x)
					complete = true
				},
				Pull: func(io IOlet) {
					m.Lock()
					defer m.Unlock()
					if complete {
						io.Complete()
						return
					}
					io.Pull()
				},
			}
		})
	})
}

func Reduce[T any](f func(T, T) T) FlowGraph {
	var t0 T
	return Fold(t0, f)
}
