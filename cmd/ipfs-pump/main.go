package main

import (
	"fmt"
	"log"

	"github.com/pkg/errors"

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
		count := 0
		for info := range infoIn {
			count++
			if info.Error != nil {
				log.Println(errors.Wrapf(err, "error enumerating block %s", info.CID))
				continue
			}
			total := enumerator.TotalCount()
			if total > 0 {
				ratio := 100. * float32(count) / float32(total)
				fmt.Printf("[%d/%d - %.2f%%] %s\n", count, total, ratio, info.CID)
			} else {
				fmt.Printf("[%d/%d] %s\n", count, total, info.CID)
			}
			infoOut <- info
		}
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
