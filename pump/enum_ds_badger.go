package pump

import (
	badger "github.com/ipfs/go-ds-badger"
	"github.com/pkg/errors"
)

func NewBadgerEnumerator(path string) (*DatastoreEnumerator, error) {
	ds, err := badger.NewDatastore(path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "Badger enumerator")
	}

	return NewDatastoreEnumerator(ds), nil
}
