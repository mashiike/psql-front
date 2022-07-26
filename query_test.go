package psqlfront_test

import (
	"testing"

	psqlfront "github.com/mashiike/psql-front"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeQuery(t *testing.T) {
	cases := []struct {
		casename    string
		query       string
		isQueryRows bool
		tables      []*psqlfront.Table
	}{
		{
			casename:    "basic select",
			query:       LoadFile(t, "testdata/sql/basic_select.sql"),
			isQueryRows: true,
			tables: []*psqlfront.Table{
				{
					SchemaName: "public",
					RelName:    "calender",
				},
			},
		},
		{
			casename:    "insert into select",
			query:       LoadFile(t, "testdata/sql/insert_into_select.sql"),
			isQueryRows: false,
		},
		{
			casename:    "with cte",
			query:       LoadFile(t, "testdata/sql/with_cte.sql"),
			isQueryRows: true,
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
			casename:    "fetch",
			query:       LoadFile(t, "testdata/sql/fetch.sql"),
			isQueryRows: true,
			tables:      []*psqlfront.Table{},
		},
		{
			casename:    "declare_cursor",
			query:       LoadFile(t, "testdata/sql/declare_cursor.sql"),
			isQueryRows: false,
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
			tables, isQueryRows, err := psqlfront.AnalyzeQuery(c.query)
			require.NoError(t, err)
			require.Equal(t, c.isQueryRows, isQueryRows)
			require.ElementsMatch(t, c.tables, tables)
		})
	}
}
