package pump

import (
	"bufio"
	"fmt"
	"os"

	"github.com/ipfs/go-cid"
)

type FailedBlocksWriter interface {
	Write(c cid.Cid) (int, error)
	Flush() error
	Count() uint
}

var _ FailedBlocksWriter = &FileEnumeratorWriter{}
var _ FailedBlocksWriter = &NullableFileEnumeratorWriter{}

type FileEnumeratorWriter struct {
	file  *bufio.Writer
	count uint
}

type NullableFileEnumeratorWriter struct {
	count uint
}

func NewFileEnumeratorWriter(path string) (enumWriter FailedBlocksWriter, close func() error, err error) {
	fo, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, err
	}

	w := bufio.NewWriter(fo)

	return &FileEnumeratorWriter{
		file:  w,
		count: 0,
	}, fo.Close, nil
}

func (f *FileEnumeratorWriter) Write(c cid.Cid) (int, error) {
	f.count++
	return f.file.WriteString(fmt.Sprintf("%v\n", c.String()))
}

func (f *FileEnumeratorWriter) Flush() error {
	return f.file.Flush()
}

func (f *FileEnumeratorWriter) Count() uint {
	return f.count
}

func NewNullableFileEnumeratorWriter() FailedBlocksWriter {
	return &NullableFileEnumeratorWriter{}
}

func (f *NullableFileEnumeratorWriter) Write(c cid.Cid) (int, error) {
	f.count++
	return 0, nil
}

func (f *NullableFileEnumeratorWriter) Flush() error {
	return nil
}

func (f *NullableFileEnumeratorWriter) Count() uint {
	return f.count
}
