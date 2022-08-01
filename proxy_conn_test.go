package psqlfront_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/lestrrat-go/backoff/v2"
	psqlfront "github.com/mashiike/psql-front"
	"github.com/stretchr/testify/require"
)

func TestProxyConn(t *testing.T) {
	cfg := preparePSQL(t)
	cert, err := tls.LoadX509KeyPair("./testdata/certificate/server.crt", "./testdata/certificate/server.key")
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	var actual string
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			client, err := listener.Accept()
			select {
			case <-ctx.Done():
				return
			default:
				require.NoError(t, err)
			}
			upstream, err := net.Dial("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.Port))
			require.NoError(t, err)
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := psqlfront.NewProxyConn(
					client, upstream,
					psqlfront.WithProxyConnTLS(&tls.Config{
						Certificates: []tls.Certificate{cert},
					}),
					psqlfront.WithProxyConnOnQueryReceived(func(_ context.Context, query string, _ bool) error {
						actual = query
						return nil
					}),
				)
				require.NoError(t, err)
				err = conn.Run(ctx)
				require.NoError(t, err)
			}()
		}
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
			"postgres://%s:%s@%s/%s?sslmode=disable",
			cfg.Username,
			cfg.Password,
			listener.Addr(),
			cfg.Database,
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
	expected := "SELECT * FROM pg_tables LIMIT $1"
	rows, err := conn.Query(ctx, expected, 1)
	require.NoError(t, err)
	values := make([][]interface{}, 0, 1)
	for rows.Next() {
		v, err := rows.Values()
		require.NoError(t, err)
		values = append(values, v)
	}
	t.Log(values)
	rows.Close()
	require.EqualValues(t, expected, actual)
	require.Equal(t, 1, len(values))
	cancel()
	listener.Close()
	wg.Wait()
}
