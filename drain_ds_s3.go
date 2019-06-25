package pump

import (
	"github.com/ipfs/go-ds-s3"
	"github.com/pkg/errors"
)

func NewS3Drain(config s3ds.Config) (*DatastoreDrain, error) {
	s3, err := s3ds.NewS3Datastore(config)
	if err != nil {
		return nil, errors.Wrap(err, "S3 drain")
	}

	return NewDatastoreDrain(s3), nil
}
