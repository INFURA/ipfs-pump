package main

import (
	"log"
	"strings"

	s3ds "github.com/ipfs/go-ds-s3"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/cheggaaa/pb.v1"

	"github.com/INFURA/ipfs-pump"
)

const (
	EnumAPIPin = "apipin"
	EnumS3     = "s3"
)

const (
	CollAPI = "api"
	CollS3  = "s3"
)

const (
	DrainAPI = "api"
	DrainS3  = "s3"
)

var (
	enumValues = []string{EnumAPIPin, EnumS3}
	enumArg    = kingpin.Arg("enum", "The source to enumerate the content. "+
		"Possible values are ["+strings.Join(enumValues, ",")+"].").
		Required().Enum(enumValues...)
	collValues = []string{CollAPI, CollS3}
	collArg    = kingpin.Arg("coll", "The source to get the data blocks. "+
		"Possible values are ["+strings.Join(collValues, ",")+"].").
		Required().Enum(collValues...)
	drainValues = []string{DrainAPI, DrainS3}
	drainArg    = kingpin.Arg("drain", "The destination to copy to. "+
		"Possible values are ["+strings.Join(drainValues, ",")+"].").
		Required().Enum(drainValues...)

	enumAPIPinURL    = kingpin.Flag("enum-api-pin-url", "Enumerator "+EnumAPIPin+": API URL")
	enumAPIPinURLVal = enumAPIPinURL.String()

	enumS3Region          = kingpin.Flag("enum-s3-region", "Enumerator "+EnumS3+": Region")
	enumS3RegionVal       = enumS3Region.String()
	enumS3Bucket          = kingpin.Flag("enum-s3-bucket", "Enumerator "+EnumS3+": Bucket name")
	enumS3BucketVal       = enumS3Bucket.String()
	enumS3AccessKey       = kingpin.Flag("enum-s3-access-key", "Enumerator "+EnumS3+": Access key")
	enumS3AccessKeyVal    = enumS3AccessKey.String()
	enumS3SecretKey       = kingpin.Flag("enum-s3-secret-key", "Enumerator "+EnumS3+": Secret key")
	enumS3SecretKeyVal    = enumS3SecretKey.String()
	enumS3SessionToken    = kingpin.Flag("enum-s3-session-token", "Enumerator "+EnumS3+": Session token")
	enumS3SessionTokenVal = enumS3SessionToken.String()

	collAPIURL    = kingpin.Flag("coll-api-url", "Collector "+CollAPI+": API URL")
	collAPIURLVal = collAPIURL.String()

	collS3Region          = kingpin.Flag("coll-s3-region", "Collector "+EnumS3+": Region")
	collS3RegionVal       = collS3Region.String()
	collS3Bucket          = kingpin.Flag("coll-s3-bucket", "Collector "+CollS3+": Bucket name")
	collS3BucketVal       = collS3Bucket.String()
	collS3AccessKey       = kingpin.Flag("coll-s3-access-key", "Collector "+CollS3+": Access key")
	collS3AccessKeyVal    = collS3AccessKey.String()
	collS3SecretKey       = kingpin.Flag("coll-s3-secret-key", "Collector "+CollS3+": Secret key")
	collS3SecretKeyVal    = collS3SecretKey.String()
	collS3SessionToken    = kingpin.Flag("coll-s3-session-token", "Collector "+CollS3+": Session token")
	collS3SessionTokenVal = collS3SessionToken.String()

	drainAPIURL    = kingpin.Flag("drain-api-url", "Drain "+DrainAPI+": API URL")
	drainAPIURLVal = drainAPIURL.String()

	drainS3Region          = kingpin.Flag("drain-s3-region", "Drain "+EnumS3+": Region")
	drainS3RegionVal       = drainS3Region.String()
	drainS3Bucket          = kingpin.Flag("drain-s3-bucket", "Drain "+DrainS3+": Bucket name")
	drainS3BucketVal       = drainS3Bucket.String()
	drainS3AccessKey       = kingpin.Flag("drain-s3-access-key", "Drain "+DrainS3+": Access key")
	drainS3AccessKeyVal    = drainS3AccessKey.String()
	drainS3SecretKey       = kingpin.Flag("drain-s3-secret-key", "Drain "+DrainS3+": Secret key")
	drainS3SecretKeyVal    = drainS3SecretKey.String()
	drainS3SessionToken    = kingpin.Flag("drain-s3-session-token", "Drain "+DrainS3+": Session token")
	drainS3SessionTokenVal = drainS3SessionToken.String()
)

func main() {
	kingpin.Parse()

	var enumerator pump.Enumerator
	var collector pump.Collector
	var drain pump.Drain
	var err error

	switch *enumArg {
	case EnumAPIPin:
		requiredFlag(enumAPIPinURL, *enumAPIPinURLVal)
		enumerator = pump.NewAPIPinEnumerator(*enumAPIPinURLVal)
	case EnumS3:
		requiredFlag(enumS3Region, *enumS3RegionVal)
		requiredFlag(enumS3Bucket, *enumS3BucketVal)

		config := s3ds.Config{
			Region:       *enumS3RegionVal,
			Bucket:       *enumS3BucketVal,
			AccessKey:    *enumS3AccessKeyVal,
			SecretKey:    *enumS3SecretKeyVal,
			SessionToken: *enumS3SessionTokenVal,
		}

		enumerator, err = pump.NewS3Enumerator(config)
	}

	if err != nil {
		log.Fatal(err)
	}

	switch *collArg {
	case CollAPI:
		requiredFlag(collAPIURL, *collAPIURLVal)
		collector = pump.NewAPICollector(*collAPIURLVal)
	case CollS3:
		requiredFlag(collS3Region, *collS3RegionVal)
		requiredFlag(collS3Bucket, *collS3BucketVal)

		config := s3ds.Config{
			Region:       *collS3RegionVal,
			Bucket:       *collS3BucketVal,
			AccessKey:    *collS3AccessKeyVal,
			SecretKey:    *collS3SecretKeyVal,
			SessionToken: *collS3SessionTokenVal,
		}

		collector, err = pump.NewS3Collector(config)
	}

	if err != nil {
		log.Fatal(err)
	}

	switch *drainArg {
	case DrainAPI:
		requiredFlag(drainAPIURL, *drainAPIURLVal)
		drain = pump.NewAPIDrain(*drainAPIURLVal)
	case DrainS3:
		requiredFlag(drainS3Region, *drainS3RegionVal)
		requiredFlag(drainS3Bucket, *drainS3BucketVal)

		config := s3ds.Config{
			Region:       *drainS3RegionVal,
			Bucket:       *drainS3BucketVal,
			AccessKey:    *drainS3AccessKeyVal,
			SecretKey:    *drainS3SecretKeyVal,
			SessionToken: *drainS3SessionTokenVal,
		}

		drain, err = pump.NewS3Drain(config)
	}

	if err != nil {
		log.Fatal(err)
	}

	PumpIt(enumerator, collector, drain)
}

func requiredFlag(flag *kingpin.FlagClause, val string) {
	if len(val) == 0 {
		log.Fatalf("flag %s is required", flag.Model().Name)
	}
}

func PumpIt(enumerator pump.Enumerator, collector pump.Collector, drain pump.Drain) {
	infoIn := make(chan pump.BlockInfo)
	infoOut := make(chan pump.BlockInfo)
	blocks := make(chan pump.Block)

	err := enumerator.CIDs(infoIn)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		bar := pb.StartNew(0)
		bar.ShowElapsedTime = true
		bar.ShowTimeLeft = true
		bar.ShowSpeed = true

		for info := range infoIn {
			bar.Increment()
			bar.SetTotal(enumerator.TotalCount())
			bar.Prefix(info.CID.String())

			if info.Error != nil {
				log.Println(errors.Wrapf(err, "error enumerating block %s", info.CID.String()))
				continue
			}

			infoOut <- info
		}
		bar.Finish()
		close(infoOut)
	}()

	err = collector.Blocks(infoOut, blocks)
	if err != nil {
		log.Fatal(err)
	}

	for block := range blocks {
		if block.Error != nil {
			log.Println(errors.Wrapf(block.Error, "error retrieving bloc %s", block.CID.String()))
			continue
		}

		err = drain.Drain(block)
		if err != nil {
			log.Println(errors.Wrapf(err, "failed to push block %s", block.CID.String()))
		}
	}
}
