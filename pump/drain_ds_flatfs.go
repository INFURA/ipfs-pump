package pump

import (
	"github.com/ipfs/go-ds-flatfs"
	"github.com/pkg/errors"
)

func NewFlatFSDrain(path string) (*DatastoreDrain, error) {
	ds, err := flatfs.Open(path, false)
	if err != nil {
		return nil, errors.Wrap(err, "FlatFS drain")
	}

	return NewDatastoreDrain(ds), nil
}
