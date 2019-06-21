package main

import (
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
