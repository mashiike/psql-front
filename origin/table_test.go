package origin_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mashiike/psql-front/origin"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
)

func TestTableFetchRows(t *testing.T) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.WriteAll([][]string{
		{"id", "name", "role"},
		{"1", "平塚 えみ", "manager"},
		{"2", "大塚 曽根吾郎", "takumi"},
		{"3", "平成 太郎", "takumi"},
		{"4", "令和 みすず", "enginner"},
	})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sjis":
			tw := transform.NewWriter(w, japanese.ShiftJIS.NewEncoder().Transformer)
			tw.Write(buf.Bytes())
			return
		case "/eucjp":
			tw := transform.NewWriter(w, japanese.EUCJP.NewEncoder().Transformer)
			tw.Write(buf.Bytes())
			return
		}
		http.NotFound(w, r)
	}))
	defer s.Close()
	columns := origin.ColumnConfigs{
		{
			Name:      "id",
			DataType:  "BIGINT",
			Contraint: "NOT NULL",
		},
		{
			Name:     "name",
			DataType: "TEXT",
		},
		{
			Name:     "role",
			DataType: "TEXT",
		},
	}
	cases := []struct {
		name     string
		cfg      *origin.RemoteTableConfig
		expected [][]interface{}
	}{
		{
			name: "sjis",
			cfg: &origin.RemoteTableConfig{
				Name:        "table",
				Columns:     columns,
				IgnoreLines: 1,
				URLString:   s.URL + "/sjis",
			},
			expected: [][]interface{}{
				{int64(1), "平塚 えみ", "manager"},
				{int64(2), "大塚 曽根吾郎", "takumi"},
				{int64(3), "平成 太郎", "takumi"},
				{int64(4), "令和 みすず", "enginner"},
			},
		},
		{
			name: "eucjp",
			cfg: &origin.RemoteTableConfig{
				Name:        "table",
				Columns:     columns,
				IgnoreLines: 1,
				URLString:   s.URL + "/eucjp",
			},
			expected: [][]interface{}{
				{int64(1), "平塚 えみ", "manager"},
				{int64(2), "大塚 曽根吾郎", "takumi"},
				{int64(3), "平成 太郎", "takumi"},
				{int64(4), "令和 みすず", "enginner"},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.cfg.Restrict([]string{"http"})
			require.NoError(t, err)
			rows, err := c.cfg.FetchRows(context.Background(), nil, nil)
			require.NoError(t, err)
			require.EqualValues(t, c.expected, rows)
		})
	}
}
