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

// ===== outline =====

type Outline interface {
	Outlet
	Commands() <-chan Command
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

// ===== pipe =====

type Pipe interface {
	Inline
	Outline
}

type pipe struct {
	commands chan Command
	events   chan Event
}

func newPipe() Pipe {
	return &pipe{
		commands: make(chan Command, 1),
		events:   make(chan Event, 1),
	}
}

func (p *pipe) Commands() <-chan Command {
	return p.commands
}

func (p *pipe) Events() <-chan Event {
	return p.events
}

func (p *pipe) Close() {
	// TODO
	// close(p.commands)
	// close(p.events)
}

func (p *pipe) Pull() {
	p.commands <- PULL
}

func (p *pipe) Cancel() {
	p.commands <- CANCEL
}

func (p *pipe) Push(v any) {
	p.events <- Event{Data: v}
}

func (p *pipe) Error(e error) {
	p.events <- Event{Error: e}
}

func (p *pipe) Complete() {
	p.events <- Event{Complete: true}
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
