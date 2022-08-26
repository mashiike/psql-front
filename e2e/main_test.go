package e2e_test

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/lestrrat-go/backoff/v2"
	psqlfront "github.com/mashiike/psql-front"
	_ "github.com/mashiike/psql-front/origin/http"
	_ "github.com/mashiike/psql-front/origin/static"
	"github.com/stretchr/testify/require"
)

type serverTestCase struct {
	Name     string
	TestFunc func(t *testing.T, ctx context.Context, conn *pgx.Conn)
}

var serverTestCases = []serverTestCase{
	{
		Name: "fetch example.fuga with cursor",
		TestFunc: func(t *testing.T, ctx context.Context, conn *pgx.Conn) {
			_, err := conn.Exec(ctx, "BEGIN;")
			require.NoError(t, err)
			_, err = conn.Exec(ctx, "SET statement_timeout = 18000;")
			log.Printf("[notice] test exec set")
			require.NoError(t, err)
			log.Printf("[notice] test exec declare")
			_, err = conn.Exec(ctx, "DECLARE cursor_test_1234 NO SCROLL CURSOR FOR SELECT ymd,name,value FROM example.fuga LIMIT 100;")
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
				{time.Date(2022, 01, 01, 0, 0, 0, 0, time.UTC), "正月", int32(0)},
				{time.Date(2022, 01, 02, 0, 0, 0, 0, time.UTC), "なにもない日", int32(1)},
			}
			require.EqualValues(t, expected, actual)
		},
	},
	{
		Name: "select example.hoge",
		TestFunc: func(t *testing.T, ctx context.Context, conn *pgx.Conn) {
			rows, err := conn.Query(ctx, "SELECT * FROM example.hoge")
			require.NoError(t, err)
			actual := make([][]interface{}, 0)
			for rows.Next() {
				values, err := rows.Values()
				log.Printf("[notice] test rows=[%v]", values)
				require.NoError(t, err)
				actual = append(actual, values)
			}
			expected := [][]interface{}{
				{time.Date(2022, 8, 11, 0, 0, 0, 0, time.UTC), "山の日", int64(25), true},
				{time.Date(2022, 8, 12, 0, 0, 0, 0, time.UTC), nil, nil, false},
			}
			require.EqualValues(t, expected, actual)
		},
	},
}

func (c *serverTestCase) Run(t *testing.T, ctx context.Context, cfg *psqlfront.Config, addr string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
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
			addr,
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
		t.Fatal("can not connect")
	}
	t.Run(c.Name, func(t *testing.T) {
		c.TestFunc(t, ctx, conn)
	})

	conn.Close(ctx)
	cancel()
}

func TestServer(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/fuga", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "text/csv")
		w.WriteHeader(http.StatusOK)
		writer := csv.NewWriter(w)
		writer.WriteAll([][]string{
			{"ymd", "name", "vaule"},
			{"2022-01-01", "正月", "0"},
			{"2022-01-02", "なにもない日", "1"},
		})
	}))
	mux.HandleFunc("/hoge", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "text/csv")
		w.WriteHeader(http.StatusOK)
		writer := csv.NewWriter(w)
		writer.WriteAll([][]string{
			{"ymd", "name", "vaule", "is_holiday"},
			{"2022-08-11", "山の日", "25", "true"},
			{"2022-08-12", "", "", "false"},
		})
	}))
	originServer := httptest.NewServer(mux)
	defer originServer.Close()
	os.Setenv("ORIGIN_SERVER_URL", originServer.URL)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		err := server.RunWithContextAndListener(ctx, listener)
		require.NoError(t, err)
	}()

	for _, c := range serverTestCases {
		c.Run(t, ctx, cfg, listener.Addr().String())
	}
	cancel()
	wg.Wait()
}
