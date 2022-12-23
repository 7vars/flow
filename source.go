package flow

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
)

// ====== handler ======

type SourceHandler interface {
	OnPull(Outlet)
	OnCancel(Outlet)
}

type SourceHandlerFunc[T any] func() (T, error)

func (shf SourceHandlerFunc[T]) OnPull(outlet Outlet) {
	t, err := shf()
	if err != nil {
		if errors.Is(err, io.EOF) {
			outlet.Complete()
			return
		}
		outlet.Error(err)
		return
	}
	outlet.Push(t)
}

func (SourceHandlerFunc[T]) OnCancel(outlet Outlet) {
	outlet.Complete()
}

// ===== builder ======

type SourceBuilder interface {
	Build() Inline
}

type SourceBuilderFunc func() Inline

func (sbf SourceBuilderFunc) Build() Inline {
	return sbf()
}

type BuildSource func() SourceHandler

func (bs BuildSource) Build() Inline {
	commands := make(chan Command)
	events := make(chan Event)

	go func(commands <-chan Command, events chan<- Event, handler SourceHandler) {
		// defer fmt.Println("DEBUG: source closed")
		defer close(events)
		for cmd := range commands {
			switch cmd {
			case PULL:
				handler.OnPull(OutletChan(events))
			case CANCEL:
				handler.OnCancel(OutletChan(events))
			}
		}
	}(commands, events, bs())

	return inline{InletChan(commands), func() { close(commands) }, events}
}

// ===== graph =====

type SourceGraph interface {
	SourceBuilder
	Graph
}

type SourceGraphFunc func() SourceBuilder

func (sg SourceGraphFunc) Build() Inline {
	return sg().Build()
}

func (sg SourceGraphFunc) Via(flow FlowBuilder) Graph {
	return SourceGraphFunc(func() SourceBuilder {
		return SourceBuilderFunc(func() Inline {
			return flow.Build(sg.Build())
		})
	})
}

func (sg SourceGraphFunc) From(source SourceBuilder) Graph {
	return Merge(sg, source)
}

func (sg SourceGraphFunc) To(sink SinkBuilder) Graph {
	if fanout, ok := sink.(*fanOut); ok {
		return fanout.From(sg)
	}
	return newStraightGraph(sg, sink)
}

func (sg SourceGraphFunc) Await() error {
	return sg.AwaitWithContext(context.Background())
}

func (sg SourceGraphFunc) AwaitWithContext(ctx context.Context) error {
	_, err := sg.ExecuteWithContext(ctx)
	return err
}

func (sg SourceGraphFunc) Run() <-chan any {
	return sg.RunWithContext(context.Background())
}

func (sg SourceGraphFunc) RunWithContext(ctx context.Context) <-chan any {
	return Empty().Build(sg.Build()).RunWithContext(ctx)
}

func (sg SourceGraphFunc) Execute() (any, error) {
	return sg.ExecuteWithContext(context.Background())
}

func (sg SourceGraphFunc) ExecuteWithContext(ctx context.Context) (any, error) {
	return execute(sg.RunWithContext(ctx))
}

// ===== sources =====

func SourceFunc[T any](f func() (T, error)) SourceGraph {
	return SourceGraphFunc(func() SourceBuilder {
		return BuildSource(func() SourceHandler {
			return SourceHandlerFunc[T](f)
		})
	})
}

func ZeroSource() SourceGraph {
	return SourceFunc(func() (any, error) {
		return nil, io.EOF
	})
}

func Slice[T any](slice []T) SourceGraph {
	return SourceGraphFunc(func() SourceBuilder {
		return BuildSource(func() SourceHandler {
			var index int64
			return SourceHandlerFunc[T](func() (T, error) {
				defer atomic.AddInt64(&index, 1)
				if idx := int(atomic.LoadInt64(&index)); idx < len(slice) {
					return slice[idx], nil
				}
				var t0 T
				return t0, io.EOF
			})
		})
	})
}
