package flow

import "context"

// ===== command =====

type Command uint8

const (
	PULL Command = iota
	CANCEL
)

// ===== event =====

type EventType uint8

const (
	PUSH EventType = iota
	ERROR
	COMPLETE
)

type Event struct {
	Data     any
	Error    error
	Complete bool
}

func (e Event) Type() EventType {
	if e.Error != nil {
		return ERROR
	}
	if e.Complete {
		return COMPLETE
	}
	return PUSH
}

// ===== outlet =====

type Outlet interface {
	Push(any)
	Error(error)
	Complete()
}

type OutletChan chan<- Event

func (oc OutletChan) Push(v any) {
	oc <- Event{Data: v}
}

func (oc OutletChan) Error(e error) {
	oc <- Event{Error: e}
}

func (oc OutletChan) Complete() {
	oc <- Event{Complete: true}
}

// ===== inlet =====

type Inlet interface {
	Pull()
	Cancel()
}

type InletChan chan<- Command

func (ic InletChan) Pull() {
	ic <- PULL
}

func (ic InletChan) Cancel() {
	ic <- CANCEL
}

// ===== iolet =====

type IOlet interface {
	Inlet
	Outlet
}

type iolet struct {
	Inlet
	Outlet
}

// ===== inline =====

type Inline interface {
	Inlet
	Close()
	Events() <-chan Event
}

type inline struct {
	Inlet
	onClose func()
	events  <-chan Event
}

func (in inline) Close() {
	in.onClose()
}

func (in inline) Events() <-chan Event {
	return in.events
}

// ===== emit ======

type Emittable interface {
	Emit(any)
	Close()
}

type EmittableInlet interface {
	Inlet
	Emittable
}

type EmittableInline interface {
	Inline
	Emittable
}

// ===== graph =====

type Graph interface {
	Runnable
	Via(FlowBuilder) Graph
	To(SinkBuilder) Graph
}

type straigtGraph struct {
	source SourceBuilder
	sink   SinkBuilder
}

func (sg straigtGraph) Via(flow FlowBuilder) Graph {
	panic("fanout not implemented") // TODO
}

func (sg straigtGraph) To(sink SinkBuilder) Graph {
	panic("fanout not implemented") // TODO
}

func (sg straigtGraph) Await() error {
	return sg.AwaitWithContext(context.Background())
}

func (sg straigtGraph) AwaitWithContext(ctx context.Context) error {
	_, err := sg.ExecuteWithContext(ctx)
	return err
}

func (sg straigtGraph) Run() <-chan any {
	return sg.RunWithContext(context.Background())
}

func (sg straigtGraph) RunWithContext(ctx context.Context) <-chan any {
	return sg.sink.Build(sg.source.Build()).RunWithContext(ctx)
}

func (sg straigtGraph) Execute() (any, error) {
	return sg.ExecuteWithContext(context.Background())
}

func (sg straigtGraph) ExecuteWithContext(ctx context.Context) (any, error) {
	return execute(sg.RunWithContext(ctx))
}
