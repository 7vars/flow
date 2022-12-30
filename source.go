package flow

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// ===== handler =====

type SourceHandler interface {
	OnPull(Outlet)
	OnCancel(Outlet)
}

type Source struct {
	DoPull   func(Outlet)
	DoCancel func(Outlet)
}

func (src Source) OnPull(out Outlet) {
	if src.DoPull != nil {
		src.DoPull(out)
		return
	}
	out.Complete()
}

func (src Source) OnCancel(out Outlet) {
	if src.DoCancel != nil {
		src.DoCancel(out)
		return
	}
	out.Complete()
}

type SourceHandlerFunc[T any] func() (T, error)

func (shf SourceHandlerFunc[T]) OnPull(out Outlet) {
	t, err := shf()
	if err != nil {
		if errors.Is(err, io.EOF) {
			out.Complete()
			return
		}
		out.Error(err)
		return
	}
	out.Push(t)
}

func (shf SourceHandlerFunc[T]) OnCancel(out Outlet) { out.Complete() }

// ===== builder =====

type SourceBuilder interface {
	Build() SourceHandler
}

type SourceBuilderFunc func() SourceHandler

func (sbf SourceBuilderFunc) Build() SourceHandler {
	return sbf()
}

// ===== process =====

type SourceProcess func(context.Context, ...Outline)

func (sp SourceProcess) Process(ctx context.Context, inlines []Inline, outlines []Outline) {
	if len(inlines) > 0 {
		panic("source: inlines not supported")
	}
	sp(ctx, outlines...)
}

// ===== sources =====

func SourceProcessor(builder SourceBuilder) Processor {
	return SourceProcess(func(ctx context.Context, outlines ...Outline) {
		kill := make(chan struct{}, 1)
		var wg sync.WaitGroup
		wg.Add(len(outlines))
		cancelWorker := make([]chan<- error, len(outlines))

		for i, outline := range outlines {
			cancel := make(chan error, 1)
			cancelWorker[i] = cancel
			go func(grp *sync.WaitGroup, out Outline, cancel <-chan error, handler SourceHandler) {
				defer grp.Done()
				var err error
				for {
					select {
					case cmd, open := <-out.Commands():
						if !open {
							return
						}
						switch cmd {
						case PULL:
							if err != nil {
								out.Error(err)
								continue
							}
							handler.OnPull(out)
						case CANCEL:
							handler.OnCancel(out)
							return
						}
					case e := <-cancel:
						if e != nil {
							err = e
						}
					}
				}
			}(&wg, outline, cancel, builder.Build())
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

func SliceSource[T any](slice []T) Processor {
	return SourceProcessor(SourceBuilderFunc(func() SourceHandler {
		var index int64
		return SourceHandlerFunc[T](func() (T, error) {
			defer atomic.AddInt64(&index, 1)
			if idx := int(atomic.LoadInt64(&index)); idx < len(slice) {
				return slice[idx], nil
			}
			var t0 T
			return t0, io.EOF
		})
	}))
}
