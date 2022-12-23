package flow

import (
	"context"
	"sort"
)

// ===== worker =====

type FanOutWorker func(Inline, ...Outline)

// ===== graph =====

type FanOutGraph interface {
	Graph
	Build(Inline) Runnable
}

type fanOut struct {
	worker FanOutWorker
	sinks  []SinkBuilder
}

func NewFanOut(worker FanOutWorker, sinks ...SinkBuilder) FanOutGraph {
	return &fanOut{
		worker: worker,
		sinks:  sinks,
	}
}

func (fo *fanOut) Build(inline Inline) Runnable {
	outlines := make([]Outline, len(fo.sinks))
	runnables := make([]Runnable, len(fo.sinks))
	for i, sink := range fo.sinks {
		pipe := newPipe()
		runnables[i] = sink.Build(pipe)
		outlines[i] = pipe
	}

	go fo.worker(inline, outlines...)

	return Runnables(runnables)
}

func (fo *fanOut) From(source SourceBuilder) Graph {
	return newStraightGraph(source, fo)
}

func (fo *fanOut) Via(flow FlowBuilder) Graph {
	panic("fanout via not supported yet") // TODO
}

func (fo *fanOut) To(sink SinkBuilder) Graph {
	fo.sinks = append(fo.sinks, sink)
	return fo
}

func (fo *fanOut) Await() error {
	return fo.AwaitWithContext(context.Background())
}

func (fo *fanOut) AwaitWithContext(ctx context.Context) error {
	_, err := fo.ExecuteWithContext(ctx)
	return err
}

func (fo *fanOut) Run() <-chan any {
	return fo.RunWithContext(context.Background())
}

func (fo *fanOut) RunWithContext(ctx context.Context) <-chan any {
	return fo.Build(ZeroSource().Build()).RunWithContext(ctx)
}

func (fo *fanOut) Execute() (any, error) {
	return fo.ExecuteWithContext(context.Background())
}

func (fo *fanOut) ExecuteWithContext(ctx context.Context) (any, error) {
	return execute(fo.RunWithContext(ctx))
}

// ===== fanouts =====

// ===== broadcast =====

func Broadcast(sinks ...SinkBuilder) FanOutGraph {
	return NewFanOut(broadcastWorker, sinks...)
}

func broadcastWorker(inline Inline, outlines ...Outline) {
	pulls := make(chan []Outlet, 1)
	go broadcastInlineWorker(pulls, inline)
	go broadcastOutlineWorker(pulls, outlines...)
}

func broadcastInlineWorker(pulls <-chan []Outlet, inline Inline) {
	defer inline.Cancel()
	send := func(evt Event, outs ...Outlet) {
		for _, out := range outs {
			switch evt.Type() {
			case PUSH:
				out.Push(evt.Data)
			case ERROR:
				out.Error(evt.Error)
			case COMPLETE:
				out.Complete()
			}
		}
	}

	for pull := range pulls {
		if len(pull) == 0 {
			continue
		}
		inline.Pull()
		evt := <-inline.Events()
		send(evt, pull...)
	}
}

func broadcastOutlineWorker(pulls chan<- []Outlet, outlines ...Outline) {
	defer close(pulls)
	for {
		if len(outlines) == 0 {
			return
		}

		outlets := make([]Outlet, 0)
		removes := make([]int, 0)
		for i, outline := range outlines {
			cmd, open := <-outline.Commands()
			if !open {
				removes = append(removes, i)
				continue
			}
			switch cmd {
			case PULL:
				outlets = append(outlets, outline)
			case CANCEL:
				// TODO if primary/last send cancel to inline
				outline.Complete()
				removes = append(removes, i)
			}
		}

		sort.Slice(removes, func(i, j int) bool {
			return removes[i] > removes[j]
		})
		for _, index := range removes {
			if index >= 0 && index < len(outlines) {
				outlines = append(outlines[:index], outlines[:index+1]...)
			}
		}

		if len(outlets) == 0 {
			return
		}

		pulls <- outlets
	}
}
