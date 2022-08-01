package psqlfront_test

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v4"
	psqlfront "github.com/mashiike/psql-front"
	"github.com/stretchr/testify/require"
)

func requirePSQL(t *testing.T) *psqlfront.CacheDatabaseConfig {
	t.Helper()
	host := os.Getenv("TEST_POSTGRES_HOST")
	if host == "" {
		t.SkipNow()
	}
	cfg := &psqlfront.CacheDatabaseConfig{
		Host:     host,
		Port:     5432,
		Username: "postgres",
		Password: "postgres",
		Database: "postgres",
		SSLMode:  "prefer",
	}
	portStr := os.Getenv("TEST_POSTGRES_PORT")
	if portStr != "" {
		v, err := strconv.ParseInt(portStr, 10, 32)
		require.NoError(t, err)
		cfg.Port = int(v)
	}
	if str := os.Getenv("TEST_POSTGRES_USER"); str != "" {
		cfg.Username = str
	}
	if str := os.Getenv("TEST_POSTGRES_PASSWORD"); str != "" {
		cfg.Password = str
	}
	if str := os.Getenv("TEST_POSTGRES_DATABASE"); str != "" {
		cfg.Database = str
	}
	if str := os.Getenv("TEST_POSTGRES_SSLMODE"); str != "" {
		cfg.SSLMode = str
	}
	t.Log("enable psql test")
	return cfg
}

func preparePSQL(t *testing.T) *psqlfront.CacheDatabaseConfig {
	t.Helper()
	cfg := requirePSQL(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, cfg.DSN())
	if err != nil {
		t.Log("can not connect", err)
		t.FailNow()
	}
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	fp, err := os.Open("testdata/sql/initialize_test_db.sql")
	require.NoError(t, err)
	defer fp.Close()
	s := bufio.NewScanner(fp)
	s.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.IndexByte(data, ';'); i >= 0 {

			return i + 1, data[0:i], nil
		}
		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	})
	for s.Scan() {
		tag, err := tx.Exec(ctx, s.Text())
		require.NoError(t, err)
		t.Log(tag.String())
	}
	require.NoError(t, tx.Commit(ctx))
	conn.Close(ctx)
	return cfg
}
