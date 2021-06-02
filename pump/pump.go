package pump

import (
	"fmt"
	"log"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	"gopkg.in/cheggaaa/pb.v1"
)

func PumpIt(enumerator Enumerator, collector Collector, drain Drain, worker uint, failedBlocksWriter FailedBlocksWriter) {
	if worker == 0 {
		log.Fatal("minimal number of worker is 1")
	}

	infoIn := make(chan BlockInfo, 500000)
	infoOut := make(chan BlockInfo)
	blocks := make(chan Block)
	failedBlocks := make(chan cid.Cid)

	// Single worker for the enumerator
	err := enumerator.CIDs(infoIn)
	if err != nil {
		log.Fatal(err)
	}

	// relay to the collector workers
	go func() {
		progress := pb.StartNew(0)
		progress.ShowElapsedTime = true
		progress.ShowTimeLeft = true
		progress.ShowSpeed = true

		for info := range infoIn {
			progress.Increment()
			progress.SetTotal(enumerator.TotalCount())

			if info.Error != nil {
				log.Println(errors.Wrapf(info.Error, "error enumerating block"))
				continue
			}

			progress.Prefix(info.CID.String())
			infoOut <- info
		}
		progress.Finish()
		close(infoOut)
	}()

	// Spawn collector workers
	var wgCollector sync.WaitGroup
	for i := uint(0); i < worker; i++ {
		wgCollector.Add(1)

		go func() {
			// each worker has its own out channel so we can detect when they are all done
			out := make(chan Block)

			err = collector.Blocks(infoOut, out)
			if err != nil {
				log.Fatal(err)
			}

			// merge the collected blocks into the single output channel
			for block := range out {
				blocks <- block
			}

			wgCollector.Done()
		}()
	}

	// Close the blocks channel when all the collector worker are done
	go func() {
		wgCollector.Wait()
		close(blocks)
	}()

	// Spawn drain workers
	var wgDrain sync.WaitGroup
	for i := uint(0); i < worker; i++ {
		wgDrain.Add(1)

		go func() {
			for block := range blocks {
				if block.Error != nil {
					log.Println(errors.Wrapf(block.Error, "error retrieving block %s", block.CID.String()))
					failedBlocks <- block.CID
					continue
				}

				err = drain.Drain(block)
				if err != nil {
					log.Println(errors.Wrapf(err, "failed to push block %s", block.CID.String()))
					failedBlocks <- block.CID
					continue
				}
			}
			wgDrain.Done()
		}()
	}

	// Spawn 1 failed blocks writer worker (is enough)
	var wgFailedBlocks sync.WaitGroup
	wgFailedBlocks.Add(1)

	go func() {
		for failedBlock := range failedBlocks {
			_, err = failedBlocksWriter.Write(failedBlock)
			if err != nil {
				log.Println(fmt.Errorf("failed to write failed block %s", failedBlock.String()))
			}
		}
		wgFailedBlocks.Done()
	}()

	// Close the failed blocks channel when all the drainer worker are done
	go func() {
		wgDrain.Wait()
		close(failedBlocks)
	}()

	// Wait for all the failed blocks writing and flush the remaining buffer to disk
	wgFailedBlocks.Wait()
	err = failedBlocksWriter.Flush()
	if err != nil {
		log.Println(fmt.Errorf("failed to flush writing of failed blocks. %v", err))
	}
}
