package flow

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEventType(t *testing.T) {
	evt := Event{Data: 42}
	assert.Equal(t, PUSH, evt.Type())
	evt = Event{Error: io.EOF}
	assert.Equal(t, ERROR, evt.Type())
	evt = Event{Complete: true}
	assert.Equal(t, COMPLETE, evt.Type())
}

// func TestProcessor(t *testing.T) {
// 	sourceProcessor := SourceProcess(func(ctx context.Context, outlines ...Outline) {
// 		kill := make(chan struct{}, 1)
// 		closeChans := make([]chan struct{}, len(outlines))
// 		for i, outline := range outlines {
// 			close := make(chan struct{})
// 			closeChans[i] = close
// 			go func(out Outline, close <-chan struct{}, kill chan<- struct{}) {
// 				value := 0
// 				for {
// 					select {
// 					case cmd, open := <-out.Commands():
// 						if !open {
// 							continue
// 						}
// 						switch cmd {
// 						case PULL:
// 							out.Push(value)
// 							value++
// 						case CANCEL:
// 							out.Complete()
// 							kill <- struct{}{}
// 						}
// 					case <-close:
// 						return
// 					}
// 				}
// 			}(outline, close, kill)
// 		}

// 		go func() {
// 			closeAll := func() {
// 				for _, close := range closeChans {
// 					close <- struct{}{}
// 				}
// 			}
// 			for {
// 				select {
// 				case <-kill:
// 					closeAll()
// 					return
// 				case <-ctx.Done():
// 					closeAll()
// 					return
// 				}
// 			}
// 		}()
// 	})

// 	proc := NewProcessor(sourceProcessor)
// 	outline, commands, events := testingOutline()
// 	proc.AppendOutline(outline)
// 	cancel := proc.Build(context.Background())
// 	defer cancel()
// 	commands <- PULL
// 	evt := <-events
// 	assert.Equal(t, PUSH, evt.Type())
// 	assert.Equal(t, 0, evt.Data)
// }
