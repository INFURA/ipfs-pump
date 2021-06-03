package pump

import "sync/atomic"

type CounterDrain struct {
	drain            Drain
	successfulBlocks uint64
}

var _ CountedDrain = (*CounterDrain)(nil)

func NewCountedDrain(drain Drain) *CounterDrain {
	return &CounterDrain{drain: drain, successfulBlocks: 0}
}

func (c *CounterDrain) Drain(block Block) error {
	err := c.drain.Drain(block)
	if err != nil {
		return err
	}

	atomic.AddUint64(&c.successfulBlocks, 1)
	return nil
}

func (c *CounterDrain) SuccessfulBlocksCount() uint64 {
	return atomic.LoadUint64(&c.successfulBlocks)
}
