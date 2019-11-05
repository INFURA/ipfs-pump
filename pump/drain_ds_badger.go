package pump

import (
	badger "github.com/ipfs/go-ds-badger"
	"github.com/pkg/errors"
)

func NewBadgerDrain(path string) (*DatastoreDrain, error) {
	ds, err := badger.NewDatastore(path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "Badger drain")
	}

	return NewDatastoreDrain(ds), nil
}
