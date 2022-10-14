package client

import (
	"context"
	"encoding/json"

	"github.com/donovanhide/eventsource"
)

type EventMayPromoteChange struct {
	ResourceName string `json:"resource_name,omitempty"`
	NodeName     string `json:"node_name,omitempty"`
	MayPromote   bool   `json:"may_promote,omitempty"`
}

// custom code

// EventProvider acts as an abstraction for an EventService. It can be swapped
// out for another EventService implementation, for example for testing.
type EventProvider interface {
	// DRBDPromotion is used to subscribe to LINSTOR DRBD Promotion events
	DRBDPromotion(ctx context.Context, lastEventId string) (*DRBDMayPromoteStream, error)
}

const mayPromoteChange = "may-promote-change"

// EventService is the service that deals with LINSTOR server side event streams.
type EventService struct {
	client *Client
}

// DRBDMayPromoteStream is a struct that contains a channel of EventMayPromoteChange events
// It has a Close() method that needs to be called/defered.
type DRBDMayPromoteStream struct {
	Events chan EventMayPromoteChange
	stream *eventsource.Stream
}

// Close is used to close the underlying stream and all Go routines
func (dmp *DRBDMayPromoteStream) Close() {
	dmp.stream.Close()
}

// suscribe handles stream creation, event splitting, and context cancelation
func (e *EventService) subscribe(ctx context.Context, url, event, lastEventId string) (*eventsource.Stream, chan interface{}, error) {
	stream, err := e.client.doEvent(ctx, url, lastEventId)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan interface{})
	go func() {
		defer close(ch)
		for {
			select {
			case ev, ok := <-stream.Events:
				if !ok { // most likely someone called Close()
					return
				}
				if ev.Event() == event {
					switch event {
					case mayPromoteChange:
						var empc EventMayPromoteChange
						if err := json.Unmarshal([]byte(ev.Data()), &empc); err == nil {
							ch <- empc
						}
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return stream, ch, nil
}

// DRBDPromotion is used to subscribe to LINSTOR DRBD Promotion events
func (e *EventService) DRBDPromotion(ctx context.Context, lastEventId string) (*DRBDMayPromoteStream, error) {
	stream, ch, err := e.subscribe(ctx, "/v1/events/drbd/promotion", mayPromoteChange, lastEventId)
	if err != nil {
		return nil, err
	}

	empch := make(chan EventMayPromoteChange)
	go func() {
		defer close(empch)
		for ev := range ch {
			if e, ok := ev.(EventMayPromoteChange); ok {
				empch <- e
			}
		}
	}()

	return &DRBDMayPromoteStream{
		Events: empch,
		stream: stream,
	}, nil
}
