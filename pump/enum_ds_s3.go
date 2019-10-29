package pump

import (
	"github.com/ipfs/go-ds-s3"
	"github.com/pkg/errors"
)

func NewS3Enumerator(config s3ds.Config) (*DatastoreEnumerator, error) {
	s3, err := s3ds.NewS3Datastore(config)
	if err != nil {
		return nil, errors.Wrap(err, "S3 enumerator")
	}

	return NewDatastoreEnumerator(s3), nil
}
