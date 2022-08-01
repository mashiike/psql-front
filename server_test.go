package psqlfront_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/lestrrat-go/backoff/v2"
	psqlfront "github.com/mashiike/psql-front"
	_ "github.com/mashiike/psql-front/origin/static"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	psqlfront.RegisterOriginType(DummyOriginType, func() psqlfront.OriginConfig {
		return &DummyOriginConfig{}
	})
	defer psqlfront.UnregisterOriginType(DummyOriginType)
	cfg := psqlfront.DefaultConfig()
	err := cfg.Load("testdata/config/default.yaml")
	require.NoError(t, err)
	cfg.CacheDatabase = preparePSQL(t)
	cfg.CacheDatabase.SSLMode = "disable"
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()
	server, err := psqlfront.New(context.Background(), cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		err := server.RunWithContextAndListener(ctx, listener)
		require.NoError(t, err)
	}()

	conn, ok := func() (*pgx.Conn, bool) {
		p := backoff.Exponential(
			backoff.WithMinInterval(200*time.Millisecond),
			backoff.WithMaxInterval(1*time.Second),
			backoff.WithJitterFactor(0.05),
			backoff.WithMaxRetries(6),
		)
		retryer := p.Start(ctx)
		dsn := fmt.Sprintf(
			"postgres://%s:%s@%s/%s?sslmode=prefer",
			cfg.CacheDatabase.Username,
			cfg.CacheDatabase.Password,
			listener.Addr(),
			cfg.CacheDatabase.Database,
		)
		t.Log("connect:", dsn)
		for backoff.Continue(retryer) {
			conn, err := pgx.Connect(ctx, dsn)
			if err == nil {
				return conn, true
			}
			t.Log(err)
		}
		return nil, false
	}()
	if !ok {
		t.Fatal(err)
	}

	_, err = conn.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "SET statement_timeout = 18000;")
	log.Printf("[notice] test exec set")
	require.NoError(t, err)
	log.Printf("[notice] test exec declare")
	_, err = conn.Exec(ctx, "DECLARE cursor_test_1234 NO SCROLL CURSOR FOR SELECT id FROM example.fuga LIMIT 100;")
	require.NoError(t, err)
	log.Printf("[notice] test query fetch")
	rows, err := conn.Query(ctx, "FETCH 5 IN cursor_test_1234;")
	require.NoError(t, err)
	actual := make([][]interface{}, 0)
	log.Printf("[notice] test query fetch read rows")
	for rows.Next() {
		values, err := rows.Values()
		log.Printf("[notice] test rows=[%v]", values)
		require.NoError(t, err)
		actual = append(actual, values)
	}
	require.NoError(t, rows.Err())
	_, err = conn.Exec(ctx, "CLOSE cursor_test_1234;")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "COMMIT;")
	require.NoError(t, err)
	expected := [][]interface{}{
		{"value_id"},
	}
	require.EqualValues(t, expected, actual)
	conn.Close(ctx)
	cancel()
	wg.Wait()
}
