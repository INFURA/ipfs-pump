package pump

import (
	"log"

	s3ds "github.com/ipfs/go-ds-s3"
)

func NewS3Collector(bucket string) *DatastoreCollector {
	config := s3ds.Config{
		Bucket: bucket,
	}

	s3, err := s3ds.NewS3Datastore(config)
	if err != nil {
		log.Fatal(err)
	}

	return NewDatastoreCollector(s3)
}
