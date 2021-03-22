package pump

import (
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

var _ Enumerator = &MockEnumerator{}
var _ Collector = &MockCollector{}
var _ Drain = &MockDrain{}

type MockEnumerator struct {
	blocks *sync.Map
	count  int
}

func NewMockEnumerator(blocks *sync.Map, count int) *MockEnumerator {
	return &MockEnumerator{blocks: blocks, count: count}
}

func (m *MockEnumerator) TotalCount() int {
	return m.count
}

func (m *MockEnumerator) CIDs(out chan<- BlockInfo) error {
	i := m.count

	pref := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}

	go func() {
		defer close(out)

		for ; i > 0; i-- {
			data := make([]byte, 10000)

			_, err := rand.Read(data)
			if err != nil {
				log.Fatal(err)
			}

			c, err := pref.Sum(data)
			if err != nil {
				log.Fatal(err)
			}

			m.blocks.Store(c.String(), data)

			out <- BlockInfo{
				CID: c,
			}
		}
	}()

	return nil
}

type MockCollector struct {
	source *sync.Map
}

func NewMockCollector(source *sync.Map) *MockCollector {
	return &MockCollector{source: source}
}

func (m *MockCollector) Blocks(in <-chan BlockInfo, out chan<- Block) error {
	go func() {
		for info := range in {
			data, ok := m.source.Load(info.CID.String())
			if !ok {
				out <- Block{CID: info.CID, Error: fmt.Errorf("unknown block")}
				continue
			}

			out <- Block{
				CID:  info.CID,
				Data: data.([]byte),
			}
		}
		close(out)
	}()

	return nil
}

type MockDrain struct {
	Drained uint32
}

type MockFailingDrain struct {
	Drained uint32

	// How many blocks we want the Drain() to simulate as failed
	BlocksToFail uint
}

func NewMockDrain() *MockDrain {
	return &MockDrain{}
}

func (m *MockDrain) Drain(block Block) error {
	atomic.AddUint32(&m.Drained, 1)
	return nil
}

func NewMockFailingDrain(blocksToFail uint) *MockFailingDrain {
	return &MockFailingDrain{BlocksToFail: blocksToFail}
}

func (m *MockFailingDrain) Drain(block Block) error {
	atomic.AddUint32(&m.Drained, 1)

	if m.BlocksToFail > 0 {
		m.BlocksToFail--
		return fmt.Errorf("mocked s3 rate limit error, please slow down")
	}

	return nil
}
