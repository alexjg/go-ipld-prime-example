package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	ipfsApi "github.com/ipfs/go-ipfs-api"
	ipld "github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"io"
	"os"
)

type Event struct {
	name     string
	previous *cid.Cid
}

func fetchEvent(shell ipfsApi.Shell, eventCid cid.Cid) (*Event, error) {
	builder := eventNodeBuilder{}
	lnk := cidlink.Link{Cid: eventCid}
	err := lnk.Load(
		context.Background(),
		ipld.LinkContext{},
		&builder,
		func(lnk ipld.Link, ctx ipld.LinkContext) (io.Reader, error) {
			theCid, ok := lnk.(cidlink.Link)
			if !ok {
				return nil, fmt.Errorf("Attempted to load a non CID link: %v", lnk)
			}
			block, err := shell.BlockGet(theCid.String())
			if err != nil {
				return nil, fmt.Errorf("error loading %v: %v", theCid.String(), err)
			}
			return bytes.NewBuffer(block), nil
		},
	)
    if err != nil {
        return nil, err
    }
    node := builder.Build()
    event := node.(*Event)
    return event, nil
}

func fetchEvents(shell ipfsApi.Shell, headCid cid.Cid) ([]Event, error) {
    result := make([]Event, 0)
    head, err := fetchEvent(shell, headCid)
    if err != nil {
        return nil, fmt.Errorf("Error  fetching head event: %v", err)
    }
    result = append(result, *head)
    for (head.previous != nil) {
        next, err := fetchEvent(shell, *head.previous)
        if err != nil {
            return nil, fmt.Errorf("Error fetching linked event: %v", err)
        }
        result = append([]Event{*next}, result...)
        head = next
    }
    return result, nil
}

func putEvent(shell *ipfsApi.Shell, event *Event) (ipld.Link, error) {
	lb := cidlink.LinkBuilder{
		Prefix: cid.Prefix{
			Version:  1, // Usually '1'.
			Codec:    cid.DagCBOR,
			MhType:   0x15, // 0x15 means "sha3-384" -- See the multicodecs table: https://github.com/multiformats/multicodec/
			MhLength: 48,   // sha3-224 hash has a 48-byte sum.
		},
	}
	return lb.Build(
		context.Background(),
		ipld.LinkContext{},
		event,
		func(ipld.LinkContext) (io.Writer, ipld.StoreCommitter, error) {
			buf := bytes.Buffer{}
			return &buf, func(lnk ipld.Link) error {
				_, err := shell.BlockPut(buf.Bytes(), "cbor", "sha3-384", lb.MhLength)
				return err
			}, nil
		},
	)
}

func nodeToGo(node ipld.Node) (interface{}, error) {
	return nil, nil
}

func goToNode(value interface{}) (ipld.Node, error) {
	return nil, nil
}

type builderState uint8

const (
	builderState_initial        builderState = iota // Waiting for a BeginMap call
	builderState_expectKey                          // waiting for an AssembleKey or AssembleEntry call
	builderState_midKey                             // waiting for an AssignString call for the next key
	builderState_expectName                         // waiting for an AssembleValue call for the name key
	builderState_midName                            // waiting for an AssignString call for the name key
	builderState_expectPrevious                     // waiting for an AssembleValue call for the previous key
	builderState_midPrevious                        // waiting for an AssignLink call for the previous key
	builderState_finished                           // finished
)

type eventNodeBuilder struct {
	state      builderState
	name       *string
	previous   *cid.Cid
}

// NodeAssembler implementation for eventNodeBuilder
func (b *eventNodeBuilder) BeginMap(sizeHint int64) (ipld.MapAssembler, error) {
	if b.state == builderState_initial {
        b.state = builderState_expectKey
		return b, nil
	} else {
		return nil, fmt.Errorf("BeginMap called on MapAssembler")
	}
}
func (b *eventNodeBuilder) BeginList(sizeHint int64) (ipld.ListAssembler, error) {
	return nil, fmt.Errorf("BeginList called on non list assembler")
}
func (b *eventNodeBuilder) AssignNull() error {
    switch b.state {
    case builderState_expectKey:
        return fmt.Errorf("AssignNull called when expecting a key")
    case builderState_midKey:
        return fmt.Errorf("AssignNull calledn when constructing a key")
    case builderState_expectName:
        return fmt.Errorf("AssignNull called when waiting to assemble name value")
    case builderState_midName:
        return fmt.Errorf("AssignNull called when constructing name value")
    case builderState_expectPrevious:
        return fmt.Errorf("AssignNull called when waiting to assemble name value")
    case builderState_midPrevious:
        b.state = builderState_expectKey
        return nil
    default:
        return fmt.Errorf("AssignNull called in incorrect context")
    }
}

func (b *eventNodeBuilder) AssignBool(bool) error {
	return fmt.Errorf("AssignBool called on EventAssembler")
}
func (b *eventNodeBuilder) AssignInt(int64) error {
	return fmt.Errorf("AssignInt called on EventAssembler")
}
func (b *eventNodeBuilder) AssignFloat(float64) error {
	return fmt.Errorf("AssignFloat called on EventAssembler")
}
func (b *eventNodeBuilder) AssignString(value string) error {
	switch b.state {
	case builderState_midKey:
		switch value {
		case "name":
			b.state = builderState_expectName
			return nil
		case "previous":
			b.state = builderState_expectPrevious
			return nil
		default:
			return fmt.Errorf("AssignString called with invalid key: %s", value)
		}
	case builderState_midName:
		b.name = &value
		b.state = builderState_expectKey
		return nil
	default:
		return fmt.Errorf("AssignString called on event builder in invalid state")
	}
}
func (b *eventNodeBuilder) AssignBytes([]byte) error {
	return fmt.Errorf("AssignFloat called on EventAssembler")
}
func (b *eventNodeBuilder) AssignLink(lnk ipld.Link) error {
	if b.state == builderState_midPrevious {
		cidlink, ok := lnk.(cidlink.Link)
		if !ok {
			return fmt.Errorf("This builder only supports cid links")
		}
		b.previous = &cidlink.Cid
		b.state = builderState_expectKey
		return nil
	}
	return fmt.Errorf("AssignFloat called on EventAssembler")
}
func (b *eventNodeBuilder) AssignNode(ipld.Node) error {
	return fmt.Errorf("AssignFloat called on EventAssembler")
}
func (b *eventNodeBuilder) Prototype() ipld.NodePrototype {
	return nil
}

func (b *eventNodeBuilder) Build() ipld.Node {
	return &Event{
		name:     *b.name,
		previous: b.previous,
	}
}
func (b *eventNodeBuilder) Reset() {
	b.state = builderState_initial
	b.name = nil
	b.previous = nil
}

//MapAssembler implementation for eventNodeBuilder
func (b *eventNodeBuilder) AssembleKey() ipld.NodeAssembler {
	b.state = builderState_midKey
	return b
}
func (b *eventNodeBuilder) AssembleValue() ipld.NodeAssembler {
	switch b.state {
	case builderState_expectName:
		b.state = builderState_midName
		return b
	case builderState_expectPrevious:
		b.state = builderState_midPrevious
		return b
	default:
		panic("misuse")
	}
}
func (b *eventNodeBuilder) AssembleEntry(k string) (ipld.NodeAssembler, error) {
	if b.state == builderState_expectKey {
		kass := b.AssembleKey()
		if err := kass.AssignString(k); err != nil {
			return nil, err
		}
		return b.AssembleValue(), nil
	}
	return nil, fmt.Errorf("AssembleEntry called on EventNodeAssembler in invalid state")
}
func (b *eventNodeBuilder) Finish() error {
	if b.state == builderState_expectKey {
		if b.name != nil {
			b.state = builderState_finished
			return nil
		}
	}
	return fmt.Errorf("Finish called on EventNodeAssembler which is missing fields")
}
func (b *eventNodeBuilder) KeyPrototype() ipld.NodePrototype {
	return basicnode.Prototype.String
}

func (b *eventNodeBuilder) ValuePrototype(k string) ipld.NodePrototype {
	if k == "name" {
		return basicnode.Prototype.String
	} else if k == "previous" {
		return basicnode.Prototype.Link
	} else {
		return nil
	}
}

// ipld.Node implementation for Event
func (e *Event) Kind() ipld.Kind {
	return ipld.Kind_Map
}
func (e *Event) LookupByString(key string) (ipld.Node, error) {
	switch key {
	case "name":
		return basicnode.NewString(e.name), nil
	case "previous":
		if e.previous != nil {
			return basicnode.NewLink(cidlink.Link{Cid: *e.previous}), nil
		} else {
			return ipld.Null, nil
		}
	default:
		return nil, fmt.Errorf("no such key: %s", key)
	}
}
func (e *Event) LookupByNode(key ipld.Node) (ipld.Node, error) {
	kstring, err := key.AsString()
	if err != nil {
		return nil, err
	}
	return e.LookupByString(kstring)
}
func (e *Event) LookupByIndex(idx int64) (ipld.Node, error) {
	return nil, fmt.Errorf("LookupByIndex on map")
}
func (e *Event) LookupBySegment(seg ipld.PathSegment) (ipld.Node, error) {
	return e.LookupByString(seg.String())
}
func (e *Event) MapIterator() ipld.MapIterator {
	return &eventMapIterator{
		event:        e,
		currentIndex: 0,
	}
}
func (e *Event) ListIterator() ipld.ListIterator {
	return nil
}
func (e *Event) Length() int64 {
	return 2
}
func (e *Event) IsAbsent() bool {
	return false
}
func (e *Event) IsNull() bool {
	return false
}
func (e *Event) AsBool() (bool, error) {
	return false, fmt.Errorf("Called AsBool on a map")
}
func (e *Event) AsInt() (int64, error) {
	return 0, fmt.Errorf("Called AsInt on a map")
}
func (e *Event) AsFloat() (float64, error) {
	return 0, fmt.Errorf("Called AsFloat on a map")
}
func (e *Event) AsString() (string, error) {
	return "", fmt.Errorf("called AsString on a map")
}
func (e *Event) AsBytes() ([]byte, error) {
	return nil, fmt.Errorf("Called AsBytes on a map")
}
func (e *Event) AsLink() (ipld.Link, error) {
	return nil, fmt.Errorf("Called AsLink on a map")
}
func (e *Event) Prototype() ipld.NodePrototype {
	return prototype_event{}
}

type eventMapIterator struct {
	event        *Event
	currentIndex uint
}

func (ei *eventMapIterator) Done() bool {
	return ei.currentIndex > 1
}
func (ei *eventMapIterator) Next() (ipld.Node, ipld.Node, error) {
	switch ei.currentIndex {
	case 0:
		ei.currentIndex += 1
		return basicnode.NewString("name"), basicnode.NewString(ei.event.name), nil
	case 1:
		ei.currentIndex += 1
		previous := ipld.Null
		if ei.event.previous != nil {
			previous = basicnode.NewLink(cidlink.Link{Cid: *ei.event.previous})
		}
		return basicnode.NewString("previous"), previous, nil
	default:
		return nil, nil, fmt.Errorf("Next() called on mapiterator which is Done")
	}
}

type prototype_event struct{}

func (p prototype_event) NewBuilder() ipld.NodeBuilder {
	return &eventNodeBuilder{}
}

func main() {
    shell := ipfsApi.NewShell("http://localhost:5001")
    if os.Args[1] == "push" { 
        var prevEventCid *cid.Cid
        if len(os.Args) > 3 {
            cidStr := os.Args[3]
            rawCid, err := cid.Decode(cidStr)
            if err != nil {
                fmt.Printf("Error parsing event CID %v", err)
            }
            prevEventCid = &rawCid
        }
        event := Event{
            name:     os.Args[2],
            previous: prevEventCid,
        }
        lnk, err := putEvent(shell, &event)
        if err != nil {
            fmt.Printf("Error putting to IPFS: %v", err)
        } else {
            fmt.Printf("Succesfully pushed to: %v", lnk)
        }
    } else {
        cidStr := os.Args[2]
        eventCid, err := cid.Decode(cidStr)
        if err != nil {
            fmt.Printf("Error parsing event CID %v", err)
        }
        events, err := fetchEvents(*shell, eventCid)
        if err != nil {
            fmt.Printf("Error fetching event: %v", err)
        } else {
            fmt.Printf("Fetched events: %v", events)
        }
    }
}
