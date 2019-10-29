package pump

import (
	"github.com/ipfs/go-ds-flatfs"
	"github.com/pkg/errors"
)

func NewFlatFSEnumerator(path string) (*DatastoreEnumerator, error) {
	ds, err := flatfs.Open(path, false)
	if err != nil {
		return nil, errors.Wrap(err, "FlatFS enumerator")
	}

	return NewDatastoreEnumerator(ds), nil
}
