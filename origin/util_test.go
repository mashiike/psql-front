package origin_test

import (
	"bytes"
	"encoding/csv"
	"testing"

	"github.com/mashiike/psql-front/origin"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
)

func TestConvertTextEncodingAutoDetect(t *testing.T) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	expected := [][]string{
		{"id", "name", "role"},
		{"1", "平塚 えみ", "manager"},
		{"2", "大塚 曽根吾郎", "takumi"},
		{"3", "平成 太郎", "takumi"},
		{"4", "令和 みすず", "enginner"},
	}
	w.WriteAll(expected)
	cases := []struct {
		name        string
		transformer transform.Transformer
	}{
		{
			name:        "sjis",
			transformer: japanese.ShiftJIS.NewEncoder().Transformer,
		},
		{
			name:        "eucjp",
			transformer: japanese.EUCJP.NewEncoder().Transformer,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var w bytes.Buffer
			tw := transform.NewWriter(&w, c.transformer)
			tw.Write(buf.Bytes())
			r := origin.ConvertTextEncoding(&w, nil)
			actual, err := csv.NewReader(r).ReadAll()
			require.NoError(t, err)
			require.EqualValues(t, expected, actual)
		})
	}
}

func pointer[T any](t T) *T {
	return &t
}

func TestPerformSchemaInference(t *testing.T) {
	cases := []struct {
		name                   string
		rows                   [][]string
		ignoreLines            int
		allowUnicodeColumnName bool
		expected               origin.ColumnConfigs
	}{
		{
			name: "default",
			rows: [][]string{
				{"id", "name", "role", "created_at", "last_active_date"},
				{"1", "平塚 えみ", "manager", "2022/01/01 12:00", "2022/08/01"},
				{"2", "大塚 曽根吾郎", "takumi", "2022-01-02 12:00", "2022/08/08T12:00:00"},
				{"3", "平成 太郎", "takumi", "2022/01/03 12:00:33", "2022-08-08"},
				{"4", "令和 みすず", "enginner", "2022/01/03T12:00", "2022-08-08 13:00"},
			},
			ignoreLines: 1,
			expected: origin.ColumnConfigs{
				{
					Name:        "id",
					DataType:    "BIGINT",
					ColumnIndex: pointer(0),
				},
				{
					Name:        "name",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(1),
				},
				{
					Name:        "role",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(2),
				},
				{
					Name:        "created_at",
					DataType:    "TIMESTAMP",
					ColumnIndex: pointer(3),
				},
				{
					Name:        "last_active_date",
					DataType:    "DATE",
					ColumnIndex: pointer(4),
				},
			},
		},
		{
			name: "fields",
			rows: [][]string{
				{"id", "name", "role"},
				{"1", "平塚 えみ", "manager"},
				{"2", "大塚 曽根吾郎", "takumi"},
				{"3", "平成 太郎", "takumi"},
				{"4", "令和 みすず", "enginner"},
			},
			ignoreLines: 0,
			expected: origin.ColumnConfigs{
				{
					Name:        "field1",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(0),
				},
				{
					Name:        "field2",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(1),
				},
				{
					Name:        "field3",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(2),
				},
			},
		},
		{
			name: "anonymous_field",
			rows: [][]string{
				{"id", "名前", "役割"},
				{"1", "平塚 えみ", "manager"},
				{"2", "大塚 曽根吾郎", "takumi"},
				{"3", "平成 太郎", "takumi"},
				{"4", "令和 みすず", "enginner"},
			},
			ignoreLines: 1,
			expected: origin.ColumnConfigs{
				{
					Name:        "id",
					DataType:    "BIGINT",
					ColumnIndex: pointer(0),
				},
				{
					Name:        "anonymous_field1",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(1),
				},
				{
					Name:        "anonymous_field2",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(2),
				},
			},
		},
		{
			name: "duplicate",
			rows: [][]string{
				{"id", "name", "name"},
				{"1", "平塚 えみ", "manager"},
				{"2", "大塚 曽根吾郎", "takumi"},
				{"3", "平成 太郎", "takumi"},
				{"4", "令和 みすず", "enginner"},
			},
			ignoreLines: 1,
			expected: origin.ColumnConfigs{
				{
					Name:        "id",
					DataType:    "BIGINT",
					ColumnIndex: pointer(0),
				},
				{
					Name:        "name",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(1),
				},
				{
					Name:        "name1",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(2),
				},
			},
		},
		{
			name: "null",
			rows: [][]string{
				{"id", "name", "name"},
				{"1", "平塚 えみ", "manager"},
				{"", "", ""},
				{"3", "平成 太郎", "takumi"},
				{"4", "令和 みすず", "enginner"},
			},
			ignoreLines: 1,
			expected: origin.ColumnConfigs{
				{
					Name:        "id",
					DataType:    "BIGINT",
					ColumnIndex: pointer(0),
				},
				{
					Name:        "name",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(1),
				},
				{
					Name:        "name1",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(2),
				},
			},
		},
		{
			name: "allow_unicode_name",
			rows: [][]string{
				{"id", "{},名前\n()'\"`", "IT役割 ?🙇♂", "㊙"},
				{"1", "平塚 えみ", "manager", "true"},
				{"2", "大塚 曽根吾郎", "takumi", "false"},
				{"3", "平成 太郎", "takumi", "true"},
				{"4", "令和 みすず", "enginner", "false"},
			},
			ignoreLines:            1,
			allowUnicodeColumnName: true,
			expected: origin.ColumnConfigs{
				{
					Name:        "id",
					DataType:    "BIGINT",
					ColumnIndex: pointer(0),
				},
				{
					Name:        "名前",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(1),
				},
				{
					Name:        "IT役割",
					DataType:    "VARCHAR",
					DataLength:  pointer(256),
					ColumnIndex: pointer(2),
				},
				{
					Name:        "秘",
					DataType:    "BOOLEAN",
					ColumnIndex: pointer(3),
				},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual, err := origin.PerformSchemaInference(c.rows, c.ignoreLines, c.allowUnicodeColumnName)
			require.NoError(t, err)
			require.EqualValues(t, c.expected, actual)
		})
	}
}
