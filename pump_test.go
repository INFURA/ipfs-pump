package main

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/INFURA/ipfs-pump/pump"
)

func TestPump(t *testing.T) {
	testPump(t, pump.NewMockDrain(), 50, 0, 1)
	testPump(t, pump.NewMockDrain(), 50, 0, 50)
	testPump(t, pump.NewMockDrain(), 50, 0, 100)
	testPump(t, pump.NewMockDrain(), 500, 0, 10)

	testPump(t, pump.NewMockFailingDrain(5), 10, 5, 1)
	testPump(t, pump.NewMockFailingDrain(50), 100, 50, 50)
	testPump(t, pump.NewMockFailingDrain(50), 50, 50, 100)
	testPump(t, pump.NewMockFailingDrain(500), 5000, 500, 100)
}

func testPump(t *testing.T, drain pump.Drain, count int, failedCount uint, worker uint) {
	tmpFailedBlocksFile := filepath.Join(os.TempDir(), strconv.Itoa(int(time.Now().Unix()+int64(count)+int64(failedCount)+int64(worker))))
	defer func() {
		err := os.Remove(tmpFailedBlocksFile)
		require.NoError(t, err)
	}()

	t.Logf("Logging failed blocks in Enumerator format to file: %v", tmpFailedBlocksFile)

	blocks := sync.Map{}

	enum := pump.NewMockEnumerator(&blocks, count)
	coll := pump.NewMockCollector(&blocks)

	failedBlocksWriter, closeWriter, err := pump.NewFileEnumeratorWriter(tmpFailedBlocksFile)
	require.NoError(t, err)

	PumpIt(enum, coll, drain, worker, failedBlocksWriter)

	mockDrain, ok := drain.(*pump.MockDrain)
	if ok {
		assert.Equal(t, uint32(count), mockDrain.Drained)
	}

	mockFailedDrain, ok := drain.(*pump.MockFailingDrain)
	if ok {
		assert.Equal(t, uint32(count), mockFailedDrain.Drained)
		assert.Equal(t, uint(0), mockFailedDrain.BlocksToFail)
		assert.Equal(t, failedCount, failedBlocksWriter.Count())
	}

	err = closeWriter()
	require.NoError(t, err)

	// Test the file enumerator was generated correctly and can be re-used to pump all the failed blocks again
	if failedCount > 0 {
		var emumFile *os.File
		emumFile, err = os.Open(tmpFailedBlocksFile)
		require.NoError(t, err)

		// Use a real file enumerator this time, not a mock
		enum, err := pump.NewFileEnumerator(emumFile)
		require.NoError(t, err)

		// But swipe the failing mocked drain with a successful one
		drain = pump.NewMockDrain()
		PumpIt(enum, coll, drain, worker, pump.NewNullableFileEnumeratorWriter())

		mockDrain, ok := drain.(*pump.MockDrain)
		if ok {
			// Assert all the previously collected failed blocks were successfully pushed now
			assert.Equal(t, uint32(failedCount), mockDrain.Drained)
		}
	}
}
