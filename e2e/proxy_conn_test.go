package e2e_test

import (
	"context"
	"crypto/tls"
	"errors"
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

type proxyConnTestCase struct {
	CertFile            string
	KeyFile             string
	TestFunc            func(t *testing.T, ctx context.Context, addr string, conn *pgx.Conn)
	OnReceived          psqlfront.ProxyConnOnQueryReceivedHandlerFunc
	PrepareProxyConn    func(t *testing.T, pconn *psqlfront.ProxyConn)
	ProxyConnErrorCheck func(t *testing.T, err error)
}

func (c *proxyConnTestCase) Run(t *testing.T) {
	cfg := preparePSQL(t)
	proxyOptFns := make([]func(*psqlfront.ProxyConnOptions), 0)
	if c.OnReceived != nil {
		proxyOptFns = append(proxyOptFns, psqlfront.WithProxyConnOnQueryReceived(c.OnReceived))
	}
	if c.CertFile != "" && c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		require.NoError(t, err)
		proxyOptFns = append(proxyOptFns, psqlfront.WithProxyConnTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
		}))
	}
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
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
				conn, err := psqlfront.NewProxyConn(client, upstream, proxyOptFns...)
				require.NoError(t, err)
				if c.PrepareProxyConn != nil {
					c.PrepareProxyConn(t, conn)
				}
				err = conn.Run(ctx)
				if c.ProxyConnErrorCheck != nil {
					c.ProxyConnErrorCheck(t, err)
				} else {
					require.NoError(t, err)
				}
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

	c.TestFunc(t, ctx, listener.Addr().String(), conn)

	conn.Close(ctx)
	cancel()
	listener.Close()
	wg.Wait()
}

func TestProxyConn(t *testing.T) {
	var actual string
	c := &proxyConnTestCase{
		CertFile: "./testdata/certificate/server.crt",
		KeyFile:  "./testdata/certificate/server.key",
		OnReceived: func(_ context.Context, query string, _ bool, _ psqlfront.Notifier) error {
			actual = query
			return nil
		},
		TestFunc: func(t *testing.T, ctx context.Context, addr string, conn *pgx.Conn) {
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
		},
	}
	c.Run(t)
}

func TestProxyConnIdleTimeout(t *testing.T) {
	c := &proxyConnTestCase{
		CertFile: "./testdata/certificate/server.crt",
		KeyFile:  "./testdata/certificate/server.key",
		PrepareProxyConn: func(t *testing.T, pconn *psqlfront.ProxyConn) {
			pconn.SetIdleTimeout(500 * time.Millisecond)
		},
		TestFunc: func(t *testing.T, ctx context.Context, addr string, conn *pgx.Conn) {
			_, err := conn.Exec(ctx, "SELECT 1;")
			require.NoError(t, err)
			time.Sleep(2 * time.Second)
			_, err = conn.Exec(ctx, "SELECT 1;")
			require.Error(t, err)
		},
		ProxyConnErrorCheck: func(t *testing.T, err error) {
			require.Error(t, err)
			var oe *net.OpError
			require.True(t, errors.As(err, &oe))
			require.True(t, oe.Timeout())
		},
	}
	c.Run(t)
}
