package flow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
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
	DoPull     func(IOlet)
	DoCancel   func(IOlet)
	DoPush     func(IOlet, any)
	DoError    func(IOlet, error)
	DoComplete func(Outlet)
}

func (f Flow) OnPull(iol IOlet) {
	if f.DoPull != nil {
		f.DoPull(iol)
		return
	}
	iol.Pull()
}

func (f Flow) OnCancel(iol IOlet) {
	if f.DoCancel != nil {
		f.DoCancel(iol)
		return
	}
	iol.Cancel()
}

func (f Flow) OnPush(iol IOlet, v any) {
	if f.DoPush != nil {
		f.DoPush(iol, v)
		return
	}
	iol.Push(v)
}

func (f Flow) OnError(iol IOlet, e error) {
	if f.DoError != nil {
		f.DoError(iol, e)
		return
	}
	iol.Error(e)
}

func (f Flow) OnComplete(out Outlet) {
	if f.DoComplete != nil {
		f.DoComplete(out)
		return
	}
	out.Complete()
}

type FlowHandlerFunc[T, K any] func(T) (K, error)

func (fhf FlowHandlerFunc[T, K]) OnPull(iol IOlet) { iol.Pull() }

func (fhf FlowHandlerFunc[T, K]) OnCancel(iol IOlet) { iol.Cancel() }

func (fhf FlowHandlerFunc[T, K]) OnPush(iol IOlet, v any) {
	if t, ok := v.(T); ok {
		k, err := fhf(t)
		if err != nil {
			if errors.Is(err, io.EOF) {
				iol.Cancel()
				return
			}
			if errors.Is(err, Next) {
				iol.Pull()
				return
			}
			fhf.OnError(iol, err)
			return
		}
		iol.Push(k)
		return
	}
	var t0 T
	fhf.OnError(iol, fmt.Errorf("flow: expect type %T got %T", t0, v))
}

func (fhf FlowHandlerFunc[T, K]) OnError(iol IOlet, e error) { iol.Error(e) }

func (fhf FlowHandlerFunc[T, K]) OnComplete(out Outlet) { out.Complete() }

// ===== builder =====

type FlowBuilder interface {
	Build() FlowHandler
}

type FlowBuilderFunc func() FlowHandler

func (fbf FlowBuilderFunc) Build() FlowHandler {
	return fbf()
}

// ===== process =====

type FlowProcess func(context.Context, []Inline, []Outline)

func (fp FlowProcess) Process(ctx context.Context, inlines []Inline, outlines []Outline) {
	icnt := len(inlines)
	ocnt := len(outlines)
	if icnt == 0 {
		panic("flow: no inlines")
	}
	if ocnt == 0 {
		panic("flow: no outlines")
	}
	if icnt != ocnt {
		panic(fmt.Sprintf("flow: inlines amount %d must equal to outline amount %d", icnt, ocnt))
	}
	fp(ctx, inlines, outlines)
}

// ===== flows =====

func FlowProcessor(builder FlowBuilder) Processor {
	return FlowProcess(func(ctx context.Context, inlines []Inline, outlines []Outline) {
		kill := make(chan struct{}, 1)
		var wg sync.WaitGroup
		icnt := len(inlines)
		wg.Add(icnt)
		cancelWorker := make([]chan<- error, icnt)

		for i := 0; i < icnt; i++ {
			cancel := make(chan error, 1)
			cancelWorker[i] = cancel
			go func(grp *sync.WaitGroup, p Pipe, cn <-chan error, handler FlowHandler) {
				defer grp.Done()
				var err error
				for {
					select {
					case cmd, open := <-p.Commands():
						if !open {
							return
						}
						switch cmd {
						case PULL:
							if err != nil {
								handler.OnError(p, err)
								continue
							}
							handler.OnPull(p)
						case CANCEL:
							handler.OnCancel(p)
						}
					case evt, open := <-p.Events():
						if !open {
							continue
						}
						switch evt.Type() {
						case PUSH:
							handler.OnPush(p, evt.Data)
						case ERROR:
							handler.OnError(p, evt.Error)
						case COMPLETE:
							handler.OnComplete(p)
						}
					case e := <-cn:
						if e != nil {
							err = e
						}
					}
				}
			}(&wg, pipe{inlines[i], outlines[i]}, cancel, builder.Build())
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
