package pump

import (
	s3ds "github.com/ipfs/go-ds-s3"
	"github.com/pkg/errors"
)

func NewS3Collector(config s3ds.Config) (*DatastoreCollector, error) {
	s3, err := s3ds.NewS3Datastore(config)
	if err != nil {
		return nil, errors.Wrap(err, "S3 collector")
	}

	return NewDatastoreCollector(s3), nil
}
