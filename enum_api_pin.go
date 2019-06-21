package pump

import shell "github.com/ipfs/go-ipfs-api"

var _ Enumerator = &APIPinEnumerator{}

type APIPinEnumerator struct {
	URL        string
	totalCount int
}

func NewAPIPinEnumerator(URL string) *APIPinEnumerator {
	return &APIPinEnumerator{
		URL:        URL,
		totalCount: -1,
	}
}

func (a *APIPinEnumerator) TotalCount() int {
	return a.totalCount
}

func (a *APIPinEnumerator) CIDs(out chan<- CID) error {
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
		for cid := range pins {
			out <- CID(cid)
		}
		close(out)
	}()

	return nil
}
