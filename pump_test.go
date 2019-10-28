package main

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/INFURA/ipfs-pump/pump"
)

func TestPump(t *testing.T) {
	testPump(t, 50, 1)
	testPump(t, 50, 50)
	testPump(t, 50, 100)
	testPump(t, 500, 10)
}

func testPump(t *testing.T, count int, worker uint) {
	blocks := sync.Map{}

	enum := pump.NewMockEnumerator(&blocks, count)
	coll := pump.NewMockCollector(&blocks)
	drain := pump.NewMockDrain()

	PumpIt(enum, coll, drain, worker)

	assert.Equal(t, uint32(count), drain.Drained)
}
