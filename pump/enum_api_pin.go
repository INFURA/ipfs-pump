package pump

import (
	"context"

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

	pinStream, err := s.PinsStream(context.Background())
	if err != nil {
		return err
	}

	// Now that we started the query we can properly count, which means we need to
	// reset the counter to zero instead of -1 (meaning unknown).
	a.totalCount = 0

	go func() {
		for pinInfo := range pinStream {
			c, err := cid.Parse(pinInfo.Cid)
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
