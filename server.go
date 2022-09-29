package psqlfront

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/Songmu/flextime"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

var (
	Version = "current"
)

type ProxyConnection struct {
	id string
	*pgx.Conn
}

func (conn *ProxyConnection) ID() string {
	return conn.id
}

func (conn *ProxyConnection) Close(ctx context.Context) error {
	return conn.Conn.Close(ctx)
}

type Server struct {
	db                   *pgxpool.Pool
	cacheTTL             map[string]time.Duration
	origins              map[string]Origin
	originIDsByTable     map[string]string
	tables               map[string]*Table
	tableCond            map[string]*sync.Cond
	tableMutex           map[string]*sync.Mutex
	tlsConfig            *tls.Config
	idleTimeout          time.Duration
	cacheControllTimeout time.Duration
	upstreamAddr         string
	statsCfg             *StatsConfig

	startedAt time.Time

	// stats values are mesure atomically
	currConnections  int64
	totalConnections int64
	queries          int64
	cacheHits        int64
	cacheMisses      int64
}

func New(ctx context.Context, cfg *Config) (*Server, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.CacheDatabase.DSN())
	if err != nil {
		return nil, fmt.Errorf("unable to parse DATABASE_URL: %w", err)
	}
	poolConfig.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		log.Printf("[debug] new server pgx connection: pid=%d", c.PgConn().PID())
		return nil
	}
	poolConfig.AfterRelease = func(c *pgx.Conn) bool {
		log.Printf("[debug] release server pgx connection: pid=%d", c.PgConn().PID())
		return true
	}
	db, err := pgxpool.ConnectConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}
	server := &Server{
		db:               db,
		cacheTTL:         make(map[string]time.Duration, len(cfg.Origins)),
		origins:          make(map[string]Origin, len(cfg.Origins)),
		originIDsByTable: make(map[string]string),
		tables:           make(map[string]*Table),
		tableCond:        make(map[string]*sync.Cond),
		tableMutex:       make(map[string]*sync.Mutex),
		upstreamAddr:     fmt.Sprintf("%s:%d", cfg.CacheDatabase.Host, cfg.CacheDatabase.Port),
		statsCfg:         cfg.Stats,
	}
	if cfg.IdleTimeout != nil {
		server.idleTimeout = *cfg.IdleTimeout
	}
	if cfg.CacheControllTimeout != nil {
		server.cacheControllTimeout = *cfg.CacheControllTimeout
	} else {
		server.cacheControllTimeout = server.idleTimeout
	}
	if len(cfg.Certificates) > 0 {
		log.Println("[info] use TLS")
		certs := make([]tls.Certificate, 0, len(cfg.Certificates))
		for _, certCfg := range cfg.Certificates {
			certs = append(certs, certCfg.certificate)
		}
		server.tlsConfig = &tls.Config{
			Certificates: certs,
		}
	}
	for _, origin := range cfg.Origins {
		server.cacheTTL[origin.ID] = *origin.TTL
		o, err := origin.NewOrigin()
		if err != nil {
			return nil, fmt.Errorf("origin `%s` initialize: %w", origin.ID, err)
		}
		server.origins[origin.ID] = o
	}
	return server, nil
}

func (server *Server) RunWithContext(ctx context.Context, address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	return server.RunWithContextAndListener(ctx, listener)
}

type psqlfrontCtxKey string

var remoteAddrCtxKey psqlfrontCtxKey = "__remote_addr"

func withRemoteAddr(ctx context.Context, remoteAddr string) context.Context {
	return context.WithValue(ctx, remoteAddrCtxKey, remoteAddr)
}

func GetRemoteAddr(ctx context.Context) string {
	remoteAddr, ok := ctx.Value(remoteAddrCtxKey).(string)
	if ok {
		return remoteAddr
	}
	return "-"
}

//go:embed sql/psqlfront.sql
var systemTableDDL string

func (server *Server) RunWithContextAndListener(ctx context.Context, listener net.Listener) error {
	log.Printf("[notice] start psql-front running version: %s", Version)
	server.startedAt = flextime.Now()
	defer listener.Close()

	scanner := bufio.NewScanner(strings.NewReader(systemTableDDL))
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
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
	for scanner.Scan() {
		sql := strings.TrimSpace(scanner.Text())
		if sql == "" {
			continue
		}
		log.Println("[debug]", sql)
		if _, err := server.db.Exec(ctx, sql); err != nil {
			return err
		}
	}

	tables := make([]*Table, 0, len(server.origins))
	for _, origin := range server.origins {
		t, err := origin.GetTables(ctx)
		if err != nil {
			return fmt.Errorf("origin_id `%s` get tables:%w", origin.ID(), err)
		}
		for _, table := range t {
			server.originIDsByTable[table.String()] = origin.ID()
			server.tables[table.String()] = table
			server.tableCond[table.String()] = sync.NewCond(&sync.Mutex{})
			server.tableMutex[table.String()] = &sync.Mutex{}
			log.Printf("[debug] %s: %d columns", table.String(), len(table.Columns))
			if table.SchemaName != "public" {
				sql := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s";`, table.SchemaName)
				log.Println("[debug]", sql)
				if _, err := server.db.Exec(ctx, sql); err != nil {
					return err
				}
			}
			ddl, err := table.GenerateDDL()
			if err != nil {
				return err
			}
			log.Println("[debug]", ddl)
			if _, err := server.db.Exec(ctx, ddl); err != nil {
				return err
			}
		}
		tables = append(tables, t...)
	}
	if err := server.analezeTables(ctx, tables); err != nil {
		return fmt.Errorf("execute initial analyze:%w", err)
	}

	opts := []func(opts *ProxyConnOptions){
		WithProxyConnOnQueryReceived(server.handleQuery),
	}
	if server.tlsConfig != nil {
		opts = append(opts, WithProxyConnTLS(server.tlsConfig))
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	if server.statsCfg.enabled() {
		wg.Add(1)
		go server.monitoring(cctx, &wg)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("[notice] PostgreSQL server is up and running at [%s]", listener.Addr())
		for {
			client, err := listener.Accept()
			if err != nil {
				select {
				case <-cctx.Done():
					cancel()
					return
				default:
					log.Printf("[error] Listener accept: %v", err)
				}
			} else {
				atomic.AddInt64(&(server.totalConnections), 1)
				atomic.AddInt64(&(server.currConnections), 1)
				remoteAddr := client.RemoteAddr().String()
				log.Printf("[notice][%s] new connection", remoteAddr)
				upstream, err := net.Dial("tcp", server.upstreamAddr)
				if err != nil {
					log.Printf("[error][%s] can not connect upstream:%v", remoteAddr, err)
					client.Close()
					atomic.AddInt64(&(server.currConnections), -1)
					continue
				}
				conn, err := NewProxyConn(client, upstream, opts...)
				if err != nil {
					log.Printf("[error][%s] can create proxy conn:%v", remoteAddr, err)
					client.Close()
					upstream.Close()
					atomic.AddInt64(&(server.currConnections), -1)
					continue
				}
				conn.SetIdleTimeout(server.idleTimeout)
				wg.Add(1)
				go func() {
					defer func() {
						log.Printf("[notice][%s] close connection", remoteAddr)
						atomic.AddInt64(&(server.currConnections), -1)
						wg.Done()
					}()
					if err := conn.Run(withRemoteAddr(cctx, remoteAddr)); err != nil {
						var oe *net.OpError
						if errors.As(err, &oe) {
							if oe.Timeout() {
								log.Printf("[warn][%s] run proxy conn:%v", remoteAddr, err)
								return
							}
						}
						log.Printf("[error][%s] run proxy conn:%v", remoteAddr, err)
					}

				}()
			}
		}
	}()

	<-ctx.Done()
	log.Println("[notice] psql-front shutdown...")
	cancel()
	listener.Close()
	wg.Wait()
	return nil
}

func (server *Server) handleQuery(ctx context.Context, query string, isPrepareStmt bool, notifier Notifier) error {
	atomic.AddInt64(&(server.queries), 1)
	remoteAddr := GetRemoteAddr(ctx)
	log.Printf("[debug][%s] analyze SQL: %s", remoteAddr, query)
	tables, err := AnalyzeQuery(query)
	if err != nil {
		log.Printf("[debug][%s] analyze SQL failed: %v", remoteAddr, err)
		return err
	}
	if len(tables) == 0 {
		return nil
	}
	log.Printf("[info][%s] referenced tables: [%s]", remoteAddr, strings.Join(lo.Map(tables, func(table *Table, _ int) string {
		return table.String()
	}), ", "))
	finished := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(withRemoteAddr(context.Background(), remoteAddr), 24*time.Hour)
		defer cancel()
		if err := server.controlCache(ctx, query, tables, notifier); err != nil {
			log.Printf("[error][%s] cache controll failed: %v", remoteAddr, err)
		}
		close(finished)
		log.Printf("[info][%s] cache controll finished", remoteAddr)
	}()
	select {
	case <-finished:
		log.Printf("[debug][%s] trap finish cache controll", remoteAddr)
	case <-time.After(server.cacheControllTimeout):
		log.Printf("[info][%s] since the timeout has arrived, cache control should be done on the background.", remoteAddr)
		notifier.Notify(ctx, &pgproto3.NoticeResponse{
			Severity: "NOTICE",
			Message:  "timeout,please retry after",
		})
	}
	return nil
}

var cacheLifecycleTable = &Table{
	SchemaName: "psqlfront",
	RelName:    "cache",
}

var statsTable = &Table{
	SchemaName: "psqlfront",
	RelName:    "stats",
}

func (server *Server) analezeTables(ctx context.Context, tables []*Table) error {
	remoteAddr := GetRemoteAddr(ctx)
	log.Printf("[debug][%s] try analyze table", remoteAddr)
	if len(tables) == 0 {
		return nil
	}
	sql := "ANALYZE " + strings.Join(lo.Map(tables, func(table *Table, _ int) string {
		return table.String()
	}), ", ") + ";"
	log.Printf("[info][%s] execute: %s", remoteAddr, sql)
	_, err := server.db.Exec(ctx, sql)
	return err
}

func (server *Server) controlCache(ctx context.Context, query string, refarencedTables []*Table, notifier Notifier) error {
	remoteAddr := GetRemoteAddr(ctx)
	log.Printf("[debug][%s] try cache control SQL: %s", remoteAddr, query)
	tables := make([]*Table, 0, len(refarencedTables))
	for _, table := range refarencedTables {
		if table.String() == cacheLifecycleTable.String() || table.String() == statsTable.String() {
			continue
		}
		if table.SchemaName == "pg_catalog" || table.SchemaName == "information_schema" {
			continue
		}
		t, ok := server.tables[table.String()]
		if !ok {
			continue
		}
		tables = append(tables, t)
	}
	if len(tables) == 0 {
		log.Printf("[info][%s] only system tables or no managed by psqlfront, no check cache", remoteAddr)
		return nil
	}
	cacheInfo, err := server.getCacheInfo(ctx, tables)
	if err != nil {
		return fmt.Errorf("get cache info:%w", err)
	}
	noHitTables := lo.Filter(tables, func(t *Table, _ int) bool {
		_, ok := cacheInfo[t.String()]
		return !ok
	})
	hitTables := lo.Filter(tables, func(t *Table, _ int) bool {
		_, ok := cacheInfo[t.String()]
		return ok
	})
	defer func() {
		if len(hitTables) > 0 {
			notifier.Notify(ctx, &pgproto3.NoticeResponse{
				Severity: "NOTICE",
				Message: fmt.Sprintf("cache hit: [%s]", strings.Join(lo.Map(hitTables, func(table *Table, _ int) string {
					return table.String()
				}), ", ")),
			})
		}
		atomic.AddInt64(&(server.cacheHits), int64(len(hitTables)))
		atomic.AddInt64(&(server.cacheMisses), int64(len(noHitTables)))
	}()
	if len(noHitTables) == 0 {
		log.Printf("[info][%s] all tables cache hit", remoteAddr)
		return nil
	}
	log.Printf("[info][%s] cache no hit tables: [%s]", remoteAddr, strings.Join(lo.Map(noHitTables, func(table *Table, _ int) string {
		return table.String()
	}), ", "))
	eg, egctx := errgroup.WithContext(ctx)
	for _, noHitTable := range noHitTables {
		t := noHitTable
		eg.Go(func() error {
			tx, err := server.db.Begin(egctx)
			log.Printf("[debug] start `%s` tx", t.String())
			if err != nil {
				return fmt.Errorf("start tx:%w", err)
			}
			var commited bool
			defer func() {
				if !commited {
					if err := tx.Rollback(ctx); err != nil {
						log.Printf("[warn][%s] %s tx rollback failed: %v", remoteAddr, t.String(), err)
					} else {
						log.Printf("[debug][%s] %s tx rollback", remoteAddr, t.String())
					}
				}
				log.Printf("[debug][%s] end `%s` tx", remoteAddr, t.String())
			}()
			if err := server.refreshCache(egctx, tx, t); err != nil {
				log.Printf("[warn][%s] %s can not refresh cache: %v", remoteAddr, t, err)
				var onfe *OriginNotFoundError
				if !errors.As(err, &onfe) {
					return fmt.Errorf("refresh cache:%w", err)
				}
				return nil
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit tx:%w", err)
			}
			commited = true
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("refresh cache failed:%w", err)
	}
	if err := server.analezeTables(ctx, noHitTables); err != nil {
		return fmt.Errorf("execute noHitTables analyze:%w", err)
	}
	return nil
}

type OriginNotFoundError struct {
	err error
}

func (onfe *OriginNotFoundError) Error() string {
	return onfe.err.Error()
}

func (onfe *OriginNotFoundError) Unwrap() error {
	return onfe.err
}

func WrapOriginNotFoundError(err error) *OriginNotFoundError {
	return &OriginNotFoundError{err: err}
}

type CacheInfo struct {
	SchemaName, TableName, OriginID string
	CachedAt, ExpiredAt             time.Time
}

var psqlQueryBuilder = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func (server *Server) getCacheInfo(ctx context.Context, tables []*Table) (map[string]*CacheInfo, error) {
	remoteAddr := GetRemoteAddr(ctx)
	log.Printf("[debug][%s] get cache info", remoteAddr)
	cond := make(sq.Or, 0, len(tables))
	for _, table := range tables {
		cond = append(cond, sq.Eq{
			"schema_name": table.SchemaName,
			"table_name":  table.RelName,
		})
	}
	sql, args, err := psqlQueryBuilder.Select(
		"schema_name",
		"table_name",
		"origin_id",
		"cached_at",
		"expired_at",
	).From(cacheLifecycleTable.String()).Where(sq.And{cond /*, sq.Expr("expired_at > NOW()")*/}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query:%w", err)
	}
	log.Printf("[debug][%s] execute: %s; %v", remoteAddr, sql, args)
	rows, err := server.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	now := flextime.Now()
	defer rows.Close()
	result := make(map[string]*CacheInfo)
	for rows.Next() {
		var cacheInfo CacheInfo
		if err := rows.Scan(
			&cacheInfo.SchemaName,
			&cacheInfo.TableName,
			&cacheInfo.OriginID,
			&cacheInfo.CachedAt,
			&cacheInfo.ExpiredAt,
		); err != nil {
			return nil, fmt.Errorf("row scan: %w", err)
		}
		t := &Table{
			SchemaName: cacheInfo.SchemaName,
			RelName:    cacheInfo.TableName,
		}
		originID, ok := server.originIDsByTable[t.String()]
		if ok && originID != cacheInfo.OriginID {
			cacheInfo.OriginID = originID
			ttl, ok := server.cacheTTL[originID]
			if ok {
				renew := cacheInfo.CachedAt.Add(ttl)
				log.Printf("[debug][%s] origin_id:%s schema_name:%s table_name:%s expred_at:%s=>%s", remoteAddr,
					cacheInfo.OriginID, cacheInfo.SchemaName, cacheInfo.TableName, cacheInfo.ExpiredAt.Format(time.RFC3339), renew.Format(time.RFC3339),
				)
				cacheInfo.ExpiredAt = renew
			}
		}

		log.Printf(
			"[debug][%s] origin_id:%s schema_name:%s table_name:%s cached_at:%s, exired_at:%s", remoteAddr,
			cacheInfo.OriginID, cacheInfo.SchemaName, cacheInfo.TableName, cacheInfo.CachedAt.Format(time.RFC3339), cacheInfo.ExpiredAt.Format(time.RFC3339),
		)

		if !now.After(cacheInfo.ExpiredAt) {
			result[t.String()] = &cacheInfo
		}
	}
	return result, nil
}

type cacheWriter struct {
	tx    pgx.Tx
	table *Table
}

func (w *cacheWriter) ReplaceCacheTable(ctx context.Context, t *Table) error {
	if w.table.String() != t.String() {
		return errors.New("table name is missmatch")
	}
	w.table.Columns = t.Columns
	w.table.Constraints = t.Constraints
	ddl, err := w.table.GenerateDDL()
	if err != nil {
		return err
	}
	dropSQL := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, t.String())

	log.Printf("[debug] execute: %s;", dropSQL)
	tag, err := w.tx.Exec(ctx, dropSQL)
	if err != nil {
		return fmt.Errorf("execute drop table `%s` query:%w", w.table, err)
	}
	log.Printf("[info] %s %s", w.table, tag.String())

	log.Printf("[debug] execute: %s;", ddl)
	tag, err = w.tx.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("execute create table `%s` query:%w", w.table, err)
	}
	log.Printf("[info] %s %s", w.table, tag.String())
	return nil
}

func (w *cacheWriter) AppendRows(ctx context.Context, rows [][]interface{}) error {
	chunk := lo.Chunk(rows, 1000)
	for _, r := range chunk {
		if err := w.appendRows(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (w *cacheWriter) appendRows(ctx context.Context, rows [][]interface{}) error {

	columns := lo.Map(w.table.Columns, func(c *Column, _ int) string {
		return `"` + strings.ToLower(c.Name) + `"`
	})
	q := psqlQueryBuilder.Insert(w.table.String()).Columns(columns...)

	for _, row := range rows {
		if len(row) != len(columns) {
			return fmt.Errorf("expected columns %d, acutal columns %d", len(columns), len(row))
		}
		q = q.Values(row...)
	}
	sql, args, err := q.ToSql()
	if err != nil {
		return fmt.Errorf("build insert into `%s` query:%w", w.table, err)
	}
	log.Printf("[debug] execute: %s; %v", sql, args)
	tag, err := w.tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("execute insert into `%s` query:%w", w.table, err)
	}
	log.Printf("[info] %s %d rows inserted", w.table, tag.RowsAffected())
	return nil
}

func (w *cacheWriter) DeleteRows(ctx context.Context) error {
	sql, args, err := psqlQueryBuilder.Delete(w.table.String()).ToSql()
	if err != nil {
		return fmt.Errorf("build delete from `%s` query:%w", w.table, err)
	}
	log.Printf("[debug] execute: %s; %v", sql, args)
	tag, err := w.tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("execute delete from `%s` query:%w", w.table, err)
	}
	log.Printf("[info] %s %d rows deleted", w.table, tag.RowsAffected())
	return nil
}

func (w *cacheWriter) TargetTable() *Table {
	return w.table
}

func (server *Server) refreshCache(ctx context.Context, tx pgx.Tx, table *Table) error {
	remoteAddr := GetRemoteAddr(ctx)
	cond, ok := server.tableCond[table.String()]
	if !ok {
		server.tableCond[table.String()] = sync.NewCond(&sync.Mutex{})
	}
	mu, ok := server.tableMutex[table.String()]
	if !ok {
		server.tableMutex[table.String()] = &sync.Mutex{}
	}
	log.Printf("[debug][%s] lock check for %s", remoteAddr, table)
	cond.L.Lock()
	if !mu.TryLock() {
		log.Printf("[info][%s] wait other refresh for %s ", remoteAddr, table)
		cond.Wait()
		log.Printf("[info][%s] finish other refresh for %s ", remoteAddr, table)
		cond.L.Unlock()
		return nil
	}
	cond.L.Unlock()
	defer func() {
		mu.Unlock()
		cond.Broadcast()
	}()

	log.Printf("[debug] refresh target %s: %d columns", table.String(), len(table.Columns))
	originID, ok := server.originIDsByTable[table.String()]
	if !ok {
		return WrapOriginNotFoundError(fmt.Errorf("table %s not found", table))
	}
	origin, ok := server.origins[originID]
	if !ok {
		return WrapOriginNotFoundError(fmt.Errorf("origin %s not found", table))
	}
	log.Printf("[info] refresh cache origin `%s`", originID)
	err := origin.RefreshCache(ctx, &cacheWriter{
		tx:    tx,
		table: table,
	})
	if err != nil {
		return fmt.Errorf("origin %s, table %s get rows:%w", originID, table, err)
	}
	ttl, ok := server.cacheTTL[originID]
	if !ok {
		return fmt.Errorf("%s's ttl not found", originID)
	}
	sql, args, err := psqlQueryBuilder.Insert(cacheLifecycleTable.String()).Columns(
		"schema_name",
		"table_name",
		"origin_id",
		"cached_at",
		"expired_at",
	).Values(
		table.SchemaName,
		table.RelName,
		originID,
		sq.Expr("NOW()"),
		sq.Expr(fmt.Sprintf("NOW() + interval '%d seconds'", int64(ttl.Seconds()))),
	).Suffix(
		"ON CONFLICT (schema_name, table_name) DO UPDATE SET origin_id=EXCLUDED.origin_id, cached_at=EXCLUDED.cached_at,expired_at=EXCLUDED.expired_at",
	).ToSql()
	if err != nil {
		return fmt.Errorf("build cache upsert `%s` query:%w", table, err)
	}
	log.Printf("[debug] execute: %s; %v", sql, args)
	tag, err := tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("execute cache upsert `%s` query:%w", table, err)
	}
	log.Printf("[info] %s %s", cacheLifecycleTable.String(), tag)
	return nil
}
