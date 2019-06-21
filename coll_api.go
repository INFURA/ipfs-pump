package pump

import shell "github.com/ipfs/go-ipfs-api"

var _ Collector = &APICollector{}

type APICollector struct {
	URL string
}

func NewAPICollector(URL string) *APICollector {
	return &APICollector{URL: URL}
}

func (a *APICollector) Blocks(in <-chan CID, out chan<- Block) error {
	s := shell.NewShell(a.URL)

	go func() {
		for cid := range in {
			data, err := s.BlockGet(string(cid))
			if err != nil {
				out <- Block{Error: err}
				continue
			}

			out <- Block{
				CID:  cid,
				Data: data,
			}
		}
		close(out)
	}()

	return nil
}
