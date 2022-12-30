package flow

import "context"

// ===== command =====

type Command uint8

const (
	PULL Command = iota
	CANCEL
)

// ===== event ======

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

func (evt Event) Type() EventType {
	if evt.Error != nil {
		return ERROR
	}
	if evt.Complete {
		return COMPLETE
	}
	return PUSH
}

// ===== inlet ======

type Inlet interface {
	Pull()
	Cancel()
}

// ===== outlet =====

type Outlet interface {
	Push(any)
	Error(error)
	Complete()
}

// ===== iolet =====

type IOlet interface {
	Inlet
	Outlet
}

// ===== inline =====

type Inline interface {
	Inlet
	Events() <-chan Event
}

// ===== outline =====

type Outline interface {
	Outlet
	Commands() <-chan Command
}

// ===== handler =====

type InletHandler struct {
	OnPush     func(Inlet, any)
	OnError    func(Inlet, error)
	OnComplete func(Inlet)
}

type OutletHandler struct {
	OnPull   func(Outlet)
	OnCancel func(Outlet)
}

// ===== pipe =====

type Pipe interface {
	Inline
	Outline
}

type pipe struct {
	Inline
	Outline
}

// ===== processor =====

type Processor interface {
	Process(context.Context, []Inline, []Outline)
}

// ===== builder =====

type ProcessorBuilder interface {
	AppendOutline(Outline)
	Build(context.Context) context.CancelFunc
}

type processorBuilder struct {
	inlines   []Inline
	outlines  []Outline
	processor Processor
}

func NewProcessor(processor Processor) ProcessorBuilder {
	return &processorBuilder{
		inlines:   make([]Inline, 0),
		outlines:  make([]Outline, 0),
		processor: processor,
	}
}

func (builder *processorBuilder) AppendOutline(outline Outline) {
	builder.outlines = append(builder.outlines, outline)
}

func (builder *processorBuilder) Build(ctx context.Context) context.CancelFunc {
	cwc, cancel := context.WithCancel(ctx)
	in := make([]Inline, len(builder.inlines))
	copy(in, builder.inlines)
	out := make([]Outline, len(builder.outlines))
	copy(out, builder.outlines)
	go builder.processor.Process(cwc, in, out)
	return cancel
}
