package pump

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPump(t *testing.T) {
	v0ProtobufSha256CidPref := cid.Prefix{Version: 0, Codec: cid.DagProtobuf, MhType: multihash.SHA2_256, MhLength: 32}
	v1CborSha3CidPref := cid.Prefix{Version: 1, Codec: cid.DagCBOR, MhType: multihash.SHA3, MhLength: 32}

	// Mainly useful for developing/debugging perspective and verifies the CID doesn't change between Enum, Coll and Drain.
	testPump(t, newMockCidPrefDrain(v0ProtobufSha256CidPref), 1, 0, 1, v0ProtobufSha256CidPref)
	testPump(t, newMockCidPrefDrain(v1CborSha3CidPref), 1, 0, 1, v1CborSha3CidPref)

	// Tests the happy path
	testPump(t, newMockDrain(), 1, 0, 1, v0ProtobufSha256CidPref)
	testPump(t, newMockDrain(), 50, 0, 50, v0ProtobufSha256CidPref)
	testPump(t, newMockDrain(), 50, 0, 100, v0ProtobufSha256CidPref)
	testPump(t, newMockDrain(), 500, 0, 10, v0ProtobufSha256CidPref)

	// Tests the collection + logging of failed CIDs
	testPump(t, NewCountedDrain(newMockFailingDrain(5)), 10, 5, 1, v0ProtobufSha256CidPref)
	testPump(t, newMockFailingDrain(50), 100, 50, 50, v0ProtobufSha256CidPref)
	testPump(t, newMockFailingDrain(50), 50, 50, 100, v0ProtobufSha256CidPref)
	testPump(t, newMockFailingDrain(500), 5000, 500, 100, v0ProtobufSha256CidPref)
}

func testPump(t *testing.T, drain Drain, count int, failedCount uint, worker uint, enumCidPref cid.Prefix) {
	tmpFailedBlocksFile := filepath.Join(os.TempDir(), strconv.Itoa(int(time.Now().Unix()+int64(count)+int64(failedCount)+int64(worker))))
	defer func() {
		err := os.Remove(tmpFailedBlocksFile)
		require.NoError(t, err)
	}()

	t.Logf("Logging failed blocks in Enumerator format to file: %v", tmpFailedBlocksFile)

	blocks := sync.Map{}

	enum := newMockEnumerator(&blocks, count, enumCidPref)
	coll := NewMockCollector(&blocks)

	pbw := NewNullProgressWriter()

	failedBlocksWriter, closeWriter, err := NewFileEnumeratorWriter(tmpFailedBlocksFile)
	require.NoError(t, err)

	PumpIt(enum, coll, drain, failedBlocksWriter, pbw, worker)

	mockedDrain, ok := drain.(*mockDrain)
	if ok {
		assert.Equal(t, uint64(count), mockedDrain.Drained)
	}

	mockFailedDrain, ok := drain.(*mockFailingDrain)
	if ok {
		assert.Equal(t, uint64(count), mockFailedDrain.Drained)
		assert.Equal(t, uint64(0), mockFailedDrain.BlocksToFail)
		assert.Equal(t, failedCount, failedBlocksWriter.Count())
	}

	counterDrain, ok := drain.(*CounterDrain)
	if ok {
		// Assert the CounterDrain doesn't count failed blocks as successful
		assert.Equal(t, counterDrain.SuccessfulBlocksCount(), uint64(count-int(failedCount)))
	}

	err = closeWriter()
	require.NoError(t, err)

	// Test the file enumerator was generated correctly and can be re-used to pump all the failed blocks again
	if failedCount > 0 {
		var emumFile *os.File
		emumFile, err = os.Open(tmpFailedBlocksFile)
		require.NoError(t, err)

		// Use a real file enumerator this time, not a mock
		enum, err := NewFileEnumerator(emumFile)
		require.NoError(t, err)

		// But swipe the failing mocked drain with a successful one
		successMockedDrain := newMockDrain()
		PumpIt(enum, coll, successMockedDrain, NewNullableFileEnumeratorWriter(), pbw, worker)

		// Assert all blocks are successfully pushed
		assert.Equal(t, uint64(failedCount), successMockedDrain.Drained)
	}
}
