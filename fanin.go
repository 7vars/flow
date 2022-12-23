package flow

import (
	"context"
	"fmt"
	"sync"
)

// ===== worker =====

type FanInWorker func(Outline, ...Inline)

// ===== graph =====

type fanIn struct {
	worker  FanInWorker
	sources []SourceBuilder
}

func NewFanIn(worker FanInWorker, sources ...SourceBuilder) Graph {
	return &fanIn{
		worker:  worker,
		sources: sources,
	}
}

func (fin *fanIn) Build() Inline {
	pipe := newPipe()
	inlines := make([]Inline, len(fin.sources))
	for i, src := range fin.sources {
		inlines[i] = src.Build()
	}

	go fin.worker(pipe, inlines...)

	return pipe
}

func (fin *fanIn) From(source SourceBuilder) Graph {
	fin.sources = append(fin.sources, source)
	return fin
}

func (fin *fanIn) Via(flow FlowBuilder) SourceGraph {
	return SourceGraphFunc(func() SourceBuilder {
		return SourceBuilderFunc(func() Inline {
			return flow.Build(fin.Build())
		})
	})
}

func (fin *fanIn) To(sink SinkBuilder) Graph {
	return newStraightGraph(fin, sink)
}

func (fin *fanIn) Await() error {
	return fin.AwaitWithContext(context.Background())
}

func (fin *fanIn) AwaitWithContext(ctx context.Context) error {
	_, err := fin.ExecuteWithContext(ctx)
	return err
}

func (fin *fanIn) Run() <-chan any {
	return fin.RunWithContext(context.Background())
}

func (fin *fanIn) RunWithContext(ctx context.Context) <-chan any {
	return fin.To(Empty()).RunWithContext(ctx)
}

func (fin *fanIn) Execute() (any, error) {
	return fin.ExecuteWithContext(context.Background())
}

func (fin *fanIn) ExecuteWithContext(ctx context.Context) (any, error) {
	return execute(fin.RunWithContext(ctx))
}

// ===== fanins =====

// ===== join & zip =====

func Join(sources ...SourceBuilder) Graph {
	return NewFanIn(joinWorker(func(a []any) (any, error) { return a, nil }), sources...)
}

func Zip(f func([]any) (any, error), sources ...SourceBuilder) Graph {
	return NewFanIn(joinWorker(f), sources...)
}

func joinWorker(f func([]any) (any, error)) FanInWorker {
	return func(outline Outline, inlines ...Inline) {
		defer func() {
			for _, inline := range inlines {
				inline.Close()
			}
		}()
		for cmd := range outline.Commands() {
			switch cmd {
			case PULL:
				if len(inlines) == 0 {
					outline.Complete()
					return
				}

				results := make([]interface{}, 0)
				errs := make([]error, 0)
				for _, inline := range inlines {
					inline.Pull()
					evt := <-inline.Events()
					switch evt.Type() {
					case PUSH:
						results = append(results, evt.Data)
					case ERROR:
						errs = append(errs, evt.Error)
					case COMPLETE:
						outline.Complete()
						return
					}
				}

				result, err := f(results)
				if err != nil {
					errs = append(errs, err)
				}

				if len(errs) > 0 {
					err := errs[0]
					for i := 1; i < len(errs); i++ {
						err = fmt.Errorf("caused by: %w", errs[i])
					}
					outline.Error(err)
					continue
				}

				outline.Push(result)
			case CANCEL:
				for _, inline := range inlines {
					inline.Cancel()
				}
				outline.Complete()
				return
			}
		}
	}
}

// ===== merge =====

func Merge(sources ...SourceBuilder) Graph {
	return NewFanIn(mergeWorker, sources...)
}

func mergeWorker(outline Outline, inlines ...Inline) {
	var wg sync.WaitGroup
	pulls := make(chan chan<- Event, 1)
	events := make(chan Event)

	wg.Add(len(inlines))
	for _, inline := range inlines {
		go mergeInlineWorker(&wg, pulls, inline)
	}

	go mergeOutlineWorker(pulls, events, outline)

	wg.Wait()
	close(events)
}

func mergeInlineWorker(wg *sync.WaitGroup, pulls <-chan chan<- Event, inline Inline) {
	defer inline.Close()
	defer wg.Done()
	for pull := range pulls {
		inline.Pull()
		evt := <-inline.Events()
		switch evt.Type() {
		case PUSH, ERROR:
			pull <- evt
		case COMPLETE:
			pull <- evt
			return
		}
	}
	inline.Cancel()
}

func mergeOutlineWorker(pulls chan<- chan<- Event, events chan Event, outline Outline) {
	defer close(pulls)
	for cmd := range outline.Commands() {
		switch cmd {
		case PULL:
		pull:
			pulls <- events
			evt, open := <-events
			if !open {
				outline.Complete()
				return
			}
			switch evt.Type() {
			case PUSH:
				outline.Push(evt.Data)
			case ERROR:
				outline.Error(evt.Error)
			case COMPLETE:
				goto pull
			}

		case CANCEL:
			outline.Complete()
			return
		}
	}
}
