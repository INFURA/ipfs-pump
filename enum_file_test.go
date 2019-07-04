package pump

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileEnumerator(t *testing.T) {
	input := `QmcbQviBDZ55DxF83rTJ7fQ9PgvbpSnhRany1FXhDD11UQ
QmcZixk3G7mmDBE7oR7MkMCeGQkzuaA5e4GS3y7szp5Tbx
Qmb3yq1VE7keU1ckMfLr3UW71gnSuz3kGE618dn1H3VYbv
QmZtUAkrdTjSh2GbvkkHcf8Y5dqh5qvZDW1eH2SsnXjJR3 garbage garbage garbage
QmSDJ8nvXh4KmpYNGFFwwKuYQRz1ZAFfUDBCRNEnmDUQNn`

	reader := bytes.NewReader([]byte(input))
	enum, err := NewFileEnumerator(reader)
	require.NoError(t, err)

	require.Equal(t, 5, enum.TotalCount())

	ch := make(chan BlockInfo)
	err = enum.CIDs(ch)
	require.NoError(t, err)

	count := 0
	for cid := range ch {
		require.NoError(t, cid.Error)
		count++
	}

	require.Equal(t, 5, count)
}
