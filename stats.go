package psqlfront

import (
	"context"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/Songmu/flextime"
	ltsv "github.com/Songmu/go-ltsv"
	"github.com/jackc/pgx/v4"
)

type ServerStats struct {
	Hostname         string `ltsv:"hostname"`
	Pid              int    `ltsv:"pid"`
	Uptime           int64  `ltsv:"uptime"`
	Time             int64  `ltsv:"time"`
	Version          string `ltsv:"version"`
	CurrConnections  int64  `ltsv:"curr_connections"`
	TotalConnections int64  `ltsv:"total_connections"`
	Queries          int64  `ltsv:"queries"`
	CacheHits        int64  `ltsv:"cache_hits"`
	CacheMisses      int64  `ltsv:"cache_misses"`
	MemoryAlloc      uint64 `ltsv:"memory_alloc"`
}

func (stats *ServerStats) String() string {
	bs, err := ltsv.Marshal(stats)
	if err != nil {
		return ""
	}
	return string(bs)
}

func (stats *ServerStats) InsertInto(ctx context.Context, tx pgx.Tx) error {
	sql, args, err := psqlQueryBuilder.Insert(statsTable.String()).Columns(
		"hostname", "pid", "uptime", "time", "version",
		"curr_connections", "total_connections",
		"queries", "cache_hits", "cache_misses", "memory_alloc",
	).Values(
		stats.Hostname, stats.Pid, stats.Uptime, time.Unix(stats.Time, 0), stats.Version,
		stats.CurrConnections, stats.TotalConnections,
		stats.Queries, stats.CacheHits, stats.CacheMisses, stats.MemoryAlloc,
	).ToSql()
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, sql, args...)
	return err
}

func (stats *ServerStats) Loatate(ctx context.Context, tx pgx.Tx) error {
	sql, args, err := psqlQueryBuilder.Delete(statsTable.String()).Where(
		sq.Lt{
			"time": sq.Expr("NOW() - interval '30 day'"),
		},
	).ToSql()
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, sql, args...)
	return err
}

// GetStats returns ServerStats of app
func (server *Server) GetStats() *ServerStats {
	now := flextime.Now()
	var hostname string
	hostname, _ = os.Hostname()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	return &ServerStats{
		Hostname:         hostname,
		Pid:              os.Getpid(),
		Uptime:           int64(now.Sub(server.startedAt).Seconds()),
		Time:             time.Now().Unix(),
		Version:          Version,
		CurrConnections:  atomic.LoadInt64(&server.currConnections),
		TotalConnections: atomic.LoadInt64(&server.totalConnections),
		Queries:          atomic.LoadInt64(&server.queries),
		CacheHits:        atomic.LoadInt64(&server.cacheHits),
		CacheMisses:      atomic.LoadInt64(&server.cacheMisses),
		MemoryAlloc:      mem.Alloc,
	}
}

func (server *Server) monitoring(ctx context.Context, wg *sync.WaitGroup) {
	ticker := time.NewTicker(server.statsCfg.MonitoringInterval)
	defer func() {
		ticker.Stop()
		wg.Done()
	}()
	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
		stats := server.GetStats()
		log.Printf("[notice] %s", stats)
		if server.statsCfg.StoreDatabase {
			tx, err := server.db.Begin(ctx)
			if err != nil {
				log.Printf("[warn] can not store stats: %v", err)
				continue
			}
			if err := stats.InsertInto(ctx, tx); err != nil {
				tx.Rollback(ctx)
				log.Printf("[warn] can not store stats: %v", err)
				continue
			}
			if stats.Uptime > 86400*30 {
				if err := stats.Loatate(ctx, tx); err != nil {
					tx.Rollback(ctx)
					log.Printf("[warn] can not stored stats loatate: %v", err)
					continue
				}
			}
			if err := tx.Commit(ctx); err != nil {
				tx.Rollback(ctx)
				log.Printf("[warn] can not store stats: %v", err)
				continue
			}
		}
	}
}
