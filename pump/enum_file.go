package pump

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
)

var _ Enumerator = &FileEnumerator{}

type FileEnumerator struct {
	file  io.ReadSeeker
	count int
}

func NewFileEnumerator(file io.ReadSeeker) (*FileEnumerator, error) {
	count := 0

	// Read the whole file a first time to count the number of entries
	fileScanner := bufio.NewScanner(file)
	for fileScanner.Scan() {
		count++
	}

	// Rewind
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	return &FileEnumerator{
		file:  file,
		count: count,
	}, nil
}

func (f *FileEnumerator) TotalCount() int {
	return f.count
}

func (f *FileEnumerator) CIDs(out chan<- BlockInfo) error {
	go func() {
		defer func() {
			if closer, ok := f.file.(io.Closer); ok {
				closer.Close()
			}
			close(out)
		}()

		fileScanner := bufio.NewScanner(f.file)
		for fileScanner.Scan() {
			split := strings.Fields(fileScanner.Text())

			if len(split) < 1 {
				out <- BlockInfo{Error: fmt.Errorf("unexpected line: %s", fileScanner.Text())}
				continue
			}

			c, err := cid.Parse(split[0])
			if err != nil {
				out <- BlockInfo{Error: errors.Wrap(err, "could not parse cid")}
				continue
			}

			out <- BlockInfo{
				CID: c,
			}
		}
	}()

	return nil
}
