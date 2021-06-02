package pump

import (
	"sync/atomic"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ipfs-ds-help"
	"github.com/pkg/errors"
)

var _ Drain = &DatastoreDrain{}

type DatastoreDrain struct {
	dstore           ds.Datastore
	successfulBlocks uint64
}

func NewDatastoreDrain(dstore ds.Datastore) *DatastoreDrain {
	return &DatastoreDrain{dstore: dstore, successfulBlocks: 0}
}

func (d *DatastoreDrain) Drain(block Block) error {
	key := dshelp.CidToDsKey(block.CID)
	err := d.dstore.Put(key, block.Data)
	if err != nil {
		return errors.Wrap(err, "datastore drain")
	}
	atomic.AddUint64(&d.successfulBlocks, 1)
	return nil
}

func (d *DatastoreDrain) SuccessfulBlocksCount() uint64 {
	return d.successfulBlocks
}
