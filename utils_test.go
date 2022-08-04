package psqlfront_test

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func LoadFile(t testing.TB, path string) string {
	t.Helper()
	require.FileExists(t, path)
	fp, err := os.Open(path)
	if err != nil {
		require.NoError(t, err)
	}
	defer fp.Close()
	bs, err := io.ReadAll(fp)
	require.NoError(t, err)
	return string(bs)
}
