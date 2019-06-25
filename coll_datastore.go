package pump

import (
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ipfs-ds-help"
	"github.com/pkg/errors"
)

var _ Collector = &DatastoreCollector{}

type DatastoreCollector struct {
	dstore ds.Datastore
}

func NewDatastoreCollector(dstore ds.Datastore) *DatastoreCollector {
	return &DatastoreCollector{dstore: dstore}
}

func (d *DatastoreCollector) Blocks(in <-chan BlockInfo, out chan<- Block) error {
	go func() {
		for info := range in {
			key := dshelp.CidToDsKey(info.CID)
			data, err := d.dstore.Get(key)
			if err != nil {
				out <- Block{CID: info.CID, Error: errors.Wrap(err, "datastore collector")}
				continue
			}

			out <- Block{
				CID:  info.CID,
				Data: data,
			}
		}
		close(out)
	}()

	return nil
}
