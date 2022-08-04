package psqlfront_test

import (
	"io"
	"log"
	"testing"

	psqlfront "github.com/mashiike/psql-front"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeQuery(t *testing.T) {
	cases := []struct {
		casename string
		query    string
		tables   []*psqlfront.Table
	}{
		{
			casename: "basic select",
			query:    LoadFile(t, "testdata/sql/basic_select.sql"),
			tables: []*psqlfront.Table{
				{
					SchemaName: "public",
					RelName:    "calender",
				},
			},
		},
		{
			casename: "insert into select",
			query:    LoadFile(t, "testdata/sql/insert_into_select.sql"),
		},
		{
			casename: "with cte",
			query:    LoadFile(t, "testdata/sql/with_cte.sql"),
			tables: []*psqlfront.Table{
				{
					SchemaName: "public",
					RelName:    "calender",
				},
				{
					SchemaName: "access",
					RelName:    "log",
				},
			},
		},
		{
			casename: "fetch",
			query:    LoadFile(t, "testdata/sql/fetch.sql"),
			tables:   []*psqlfront.Table{},
		},
		{
			casename: "declare_cursor",
			query:    LoadFile(t, "testdata/sql/declare_cursor.sql"),
			tables: []*psqlfront.Table{
				{
					SchemaName: "example",
					RelName:    "fuga",
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.casename, func(t *testing.T) {
			tables, err := psqlfront.AnalyzeQuery(c.query)
			require.NoError(t, err)
			require.ElementsMatch(t, c.tables, tables)
		})
	}
}

func BenchmarkAnalyzeQuery(b *testing.B) {
	original := log.Default().Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(original)
	query := LoadFile(b, "testdata/sql/declare_cursor.sql")
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := psqlfront.AnalyzeQuery(query)
			require.NoError(b, err)
		}
	})
}
