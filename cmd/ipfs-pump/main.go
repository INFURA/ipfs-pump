package main

import (
	"log"

	"github.com/pkg/errors"
	"gopkg.in/cheggaaa/pb.v1"

	"github.com/INFURA/ipfs-pump"
)

func main() {
	sourceURL := "127.0.0.1:5001"
	drainURL := "127.0.0.1:5001"

	enumerator := pump.NewAPIPinEnumerator(sourceURL)
	collector := pump.NewAPICollector(sourceURL)
	drain := pump.NewAPIDrain(drainURL)

	PumpIt(enumerator, collector, drain)
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
				log.Println(errors.Wrapf(err, "error enumerating block %s", info.CID))
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
		err = drain.Drain(block)
		if err != nil {
			log.Println(errors.Wrapf(err, "failed to push block %s", block.CID))
		}
	}
}
