package pump

import (
	"github.com/ipfs/go-ds-flatfs"
	"github.com/pkg/errors"
)

func NewFlatFSCollector(path string) (*DatastoreCollector, error) {
	ds, err := flatfs.Open(path, false)
	if err != nil {
		return nil, errors.Wrap(err, "FlatFS collector")
	}

	return NewDatastoreCollector(ds), nil
}
