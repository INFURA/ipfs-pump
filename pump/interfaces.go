package pump

import "github.com/ipfs/go-cid"

type BlockInfo struct {
	Error error
	CID   cid.Cid
}

// An Enumerator is able to enumerate the blocks from a source
type Enumerator interface {
	// TotalCount return the total number of existing blocks,
	// or -1 if unknown/unsupported.
	TotalCount() int

	// CIDs emit in the given channel each CID existing in the source
	CIDs(out chan<- BlockInfo) error
}

type Block struct {
	Error error
	CID   cid.Cid
	Data  []byte
}

// A Collector is able to read a block from a source
type Collector interface {
	// Blocks read each CID from the input, retrieve the corresponding
	// block and emit it to the output
	Blocks(in <-chan BlockInfo, out chan<- Block) error
}

// A Drain is able to write a block to a destination
type Drain interface {
	Drain(block Block) error
	SuccessfulBlocksCount() uint64
}
