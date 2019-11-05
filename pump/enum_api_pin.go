package pump

import (
	"context"
	"encoding/json"

	"github.com/ipfs/go-cid"
	shell "github.com/ipfs/go-ipfs-api"
)

var _ Enumerator = &APIPinEnumerator{}

type APIPinEnumerator struct {
	URL        string
	stream     bool
	totalCount int
}

func NewAPIPinEnumerator(URL string, stream bool) *APIPinEnumerator {
	return &APIPinEnumerator{
		URL:        URL,
		stream:     stream,
		totalCount: -1,
	}
}

func (a *APIPinEnumerator) TotalCount() int {
	return a.totalCount
}

func (a *APIPinEnumerator) CIDs(out chan<- BlockInfo) error {
	if a.stream {
		return a.streamCIDs(out)
	} else {
		return a.directCIDs(out)
	}
}

func (a *APIPinEnumerator) directCIDs(out chan<- BlockInfo) error {
	s := shell.NewShell(a.URL)

	// Due to https://github.com/ipfs/go-ipfs/issues/6304 this can be *very* slow
	// because the server has to build the full list before starting to output the
	// first value :(
	pins, err := s.Pins()
	if err != nil {
		return err
	}

	a.totalCount = len(pins)

	go func() {
		for str := range pins {
			c, err := cid.Parse(str)
			if err != nil {
				out <- BlockInfo{Error: err}
				continue
			}

			out <- BlockInfo{CID: c}
		}
		close(out)
	}()

	return nil
}

func (a *APIPinEnumerator) streamCIDs(out chan<- BlockInfo) error {
	s := shell.NewShell(a.URL)

	pins, err := pinsStream(s)
	if err != nil {
		return err
	}

	go func() {
		for str := range pins {
			c, err := cid.Parse(str)
			if err != nil {
				out <- BlockInfo{Error: err}
				continue
			}

			out <- BlockInfo{CID: c}
			a.totalCount++
		}
		close(out)
	}()

	return nil
}

// Support for "pin ls --stream" is pending in go-ipfs-api
// See https://github.com/ipfs/go-ipfs-api/pull/190
// Support implemented here for convenience.

// pinStreamInfo is the output type for pinsStream
type pinStreamInfo struct {
	Cid  string
	Type string
}

// pinsStream is a streamed version of Pins. It returns a channel of the pins
// with their type, one of DirectPin, RecursivePin, or IndirectPin.
func pinsStream(s *shell.Shell) (<-chan pinStreamInfo, error) {
	resp, err := s.Request("pin/ls").
		Option("stream", true).
		Option("offline", true).
		Send(context.Background())
	if err != nil {
		return nil, err
	}

	if resp.Error != nil {
		resp.Close()
		return nil, resp.Error
	}

	out := make(chan pinStreamInfo)
	go func() {
		defer resp.Close()
		var pin pinStreamInfo
		defer close(out)
		dec := json.NewDecoder(resp.Output)
		for {
			err := dec.Decode(&pin)
			if err != nil {
				return
			}
			out <- pin
		}
	}()

	return out, nil
}
