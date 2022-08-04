package psqlfront

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
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
	db               *pgxpool.Pool
	cacheTTL         map[string]time.Duration
	origins          map[string]Origin
	originIDsByTable map[string]string
	tables           map[string]*Table
	migrator         *Migrator
	tlsConfig        *tls.Config
	idleTimeout      time.Duration
	upstreamAddr     string
}

func New(ctx context.Context, cfg *Config) (*Server, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.CacheDatabase.DSN())
	if err != nil {
		return nil, fmt.Errorf("unable to parse DATABASE_URL: %w", err)
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
		migrator:         NewMigrator(cfg.CacheDatabase),
		upstreamAddr:     fmt.Sprintf("%s:%d", cfg.CacheDatabase.Host, cfg.CacheDatabase.Port),
	}
	if cfg.IdleTimeout != nil {
		server.idleTimeout = *cfg.IdleTimeout
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

func (server *Server) RunWithContextAndListener(ctx context.Context, listener net.Listener) error {
	log.Printf("[notice] start psql-front running version: %s", Version)
	defer listener.Close()
	tables := make([]*Table, 0, len(server.origins))
	for _, origin := range server.origins {
		t, err := origin.GetTables(ctx)
		if err != nil {
			return fmt.Errorf("origin_id `%s` get tables:%w", origin.ID(), err)
		}
		for _, table := range t {
			server.originIDsByTable[table.String()] = origin.ID()
			server.tables[table.String()] = table
			log.Printf("[debug] %s: %d columns", table.String(), len(table.Columns))
		}
		tables = append(tables, t...)
	}
	if err := server.migrator.ExecuteMigration(tables); err != nil {
		return fmt.Errorf("execute migration:%w", err)
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
				remoteAddr := client.RemoteAddr().String()
				log.Printf("[notice][%s] new connection", remoteAddr)
				upstream, err := net.Dial("tcp", server.upstreamAddr)
				if err != nil {
					log.Printf("[error][%s] can not connect upstream:%v", remoteAddr, err)
					client.Close()
					continue
				}
				conn, err := NewProxyConn(client, upstream, opts...)
				if err != nil {
					log.Printf("[error][%s] can create proxy conn:%v", remoteAddr, err)
					client.Close()
					upstream.Close()
					continue
				}
				conn.SetIdleTimeout(server.idleTimeout)
				wg.Add(1)
				go func() {
					defer func() {
						log.Printf("[notice][%s] close connection", remoteAddr)
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
	if err := server.controlCache(ctx, query, tables, notifier); err != nil {
		return fmt.Errorf("control cache failed: %w", err)
	}
	return nil
}

var cacheLifecycleTable = &Table{
	SchemaName: "psqlfront",
	RelName:    "cache",
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
		if table.String() == cacheLifecycleTable.String() {
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
			var rollbacked bool
			defer func() {
				if !commited && !rollbacked {
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
				if err := tx.Rollback(ctx); err != nil {
					log.Printf("[warn][%s] %s tx rollback failed: %v", remoteAddr, t.String(), err)
				} else {
					log.Printf("[debug][%s] %s tx rollback", remoteAddr, t.String())
				}
				rollbacked = true
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

func (w *cacheWriter) AppendRows(ctx context.Context, rows [][]interface{}) error {

	columns := lo.Map(w.table.Columns, func(c *Column, _ int) string {
		return c.Name
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

func (server *Server) refreshCache(ctx context.Context, tx pgx.Tx, table *Table) error {
	log.Printf("[debug] refresh target %s: %d columns", table.String(), len(table.Columns))
	originID, ok := server.originIDsByTable[table.String()]
	if !ok {
		return WrapOriginNotFoundError(fmt.Errorf("table %s not found", table))
	}
	origin, ok := server.origins[originID]
	if !ok {
		return WrapOriginNotFoundError(fmt.Errorf("origin %s not found", table))
	}
	sql, args, err := psqlQueryBuilder.Delete(table.String()).ToSql()
	if err != nil {
		return fmt.Errorf("build delete from `%s` query:%w", table, err)
	}
	log.Printf("[debug] execute: %s; %v", sql, args)
	tag, err := tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("execute delete from `%s` query:%w", table, err)
	}
	log.Printf("[info] %s %d rows deleted", table, tag.RowsAffected())
	log.Printf("[info] get rows from origin `%s`", originID)
	err = origin.GetRows(ctx, &cacheWriter{
		tx:    tx,
		table: table,
	}, table)
	if err != nil {
		return fmt.Errorf("origin %s, table %s get rows:%w", originID, table, err)
	}
	ttl, ok := server.cacheTTL[originID]
	if !ok {
		return fmt.Errorf("%s's ttl not found", originID)
	}
	sql, args, err = psqlQueryBuilder.Insert(cacheLifecycleTable.String()).Columns(
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
	tag, err = tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("execute cache upsert `%s` query:%w", table, err)
	}
	log.Printf("[info] %s %s", cacheLifecycleTable.String(), tag)
	return nil
}
