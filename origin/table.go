package origin

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	psqlfront "github.com/mashiike/psql-front"
	"github.com/mattn/go-encoding"
	"github.com/saintfish/chardet"
	"github.com/samber/lo"
)

type RemoteTableConfig struct {
	Name    string        `yaml:"name,omitempty"`
	Columns ColumnConfigs `yaml:"columns,omitempty"`

	URLString    string   `yaml:"url"`
	IgnoreLines  int      `yaml:"ignore_lines"`
	TextEncoding *string  `yaml:"text_encoding"`
	URL          *url.URL `yaml:"-"`
}

func (cfg *RemoteTableConfig) Restrict(allowedSchemas []string) error {
	if cfg.Name == "" {
		return fmt.Errorf("name is required")
	}
	if cfg.URLString == "" {
		return fmt.Errorf("url is required")
	}
	var err error
	if cfg.URL, err = url.Parse(cfg.URLString); err != nil {
		return fmt.Errorf("url is invalid: %v", err)
	}
	if !lo.Contains(allowedSchemas, cfg.URL.Scheme) {
		return fmt.Errorf("url.schema must %s", strings.Join(allowedSchemas, "/"))
	}
	if len(cfg.Columns) == 0 {
		return fmt.Errorf("columns: empty")
	}
	if err := cfg.Columns.Restrict(); err != nil {
		return err
	}
	return nil
}

func (cfg *RemoteTableConfig) FetchRows(ctx context.Context, fetcher func(context.Context, *url.URL) (io.ReadCloser, error), parser func(context.Context, io.Reader) ([][]string, error)) ([][]interface{}, error) {
	remoteAddr := psqlfront.GetRemoteAddr(ctx)
	if fetcher == nil {
		fetcher = func(ctx context.Context, u *url.URL) (io.ReadCloser, error) {
			log.Printf("[debug][%s] http request: GET %s", remoteAddr, u)
			resp, err := http.Get(u.String())
			if err != nil {
				return nil, fmt.Errorf("GET %s failed: %v", u, err)
			}
			if resp.StatusCode < http.StatusOK && resp.StatusCode >= http.StatusBadRequest {
				return nil, fmt.Errorf("GET %s failed: %d %s", u, resp.StatusCode, resp.Status)
			}
			return resp.Body, nil
		}
	}
	reader, err := fetcher(ctx, cfg.URL)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	br := bufio.NewReader(reader)

	var textEncodings []string
	if cfg.TextEncoding != nil {
		textEncodings = []string{*cfg.TextEncoding}
	} else {
		det := chardet.NewTextDetector()
		if data, err := br.Peek(2048); err == nil || err == io.EOF {
			if res, err := det.DetectAll(data); err == nil {
				textEncodings = append(textEncodings, lo.Map(res, func(r chardet.Result, _ int) string {
					return r.Charset
				})...)
			}
		}
	}
	var r io.Reader = br
	if len(textEncodings) > 0 {
		for _, textEncoding := range textEncodings {
			if enc := encoding.GetEncoding(textEncoding); enc != nil {
				r = enc.NewDecoder().Reader(br)
				break
			}
		}
	}
	if parser == nil {
		parser = func(_ context.Context, r io.Reader) ([][]string, error) {
			reader := csv.NewReader(r)
			return reader.ReadAll()
		}
	}
	records, err := parser(ctx, r)
	if err != nil {
		return nil, fmt.Errorf("GET %s parse failed: %v", cfg.URL, err)
	}
	return cfg.Columns.ToRows(records, cfg.IgnoreLines), nil
}

func (cfg *RemoteTableConfig) ToTable(schema string) *psqlfront.Table {
	return &psqlfront.Table{
		SchemaName: schema,
		RelName:    cfg.Name,
		Columns:    cfg.Columns.ToColumns(),
	}
}
