package psqlfront

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgproto3/v2"
	"golang.org/x/sync/errgroup"
)

type ProxyConnOnQueryReceivedHandlerFunc func(ctx context.Context, query string, isPreparedStmt bool, notifier Notifier) error

type Notifier interface {
	Notify(ctx context.Context, resp *pgproto3.NoticeResponse) error
}

type notifier struct {
	backend *pgproto3.Backend
}

func (n *notifier) Notify(ctx context.Context, resp *pgproto3.NoticeResponse) error {
	return n.backend.Send(resp)
}

type ProxyConnOptions struct {
	tlsConfig              *tls.Config
	onQueryReceivedHandler ProxyConnOnQueryReceivedHandlerFunc
}

type ProxyConn struct {
	backend     *pgproto3.Backend
	frontend    *pgproto3.Frontend
	opts        *ProxyConnOptions
	client      net.Conn
	upstream    net.Conn
	idleTimeout time.Duration
	cancel      context.CancelFunc
	isClosed    bool
}

func WithProxyConnTLS(tlsConfig *tls.Config) func(opts *ProxyConnOptions) {
	return func(opts *ProxyConnOptions) {
		opts.tlsConfig = tlsConfig
	}
}

func WithProxyConnOnQueryReceived(handler ProxyConnOnQueryReceivedHandlerFunc) func(opts *ProxyConnOptions) {
	return func(opts *ProxyConnOptions) {
		opts.onQueryReceivedHandler = handler
	}
}

func NewProxyConn(client net.Conn, upstream net.Conn, optFns ...func(opts *ProxyConnOptions)) (*ProxyConn, error) {
	conn := &ProxyConn{
		backend:  pgproto3.NewBackend(pgproto3.NewChunkReader(client), client),
		frontend: pgproto3.NewFrontend(pgproto3.NewChunkReader(upstream), upstream),
		opts:     &ProxyConnOptions{},
		client:   client,
		upstream: upstream,
	}
	for _, optFn := range optFns {
		optFn(conn.opts)
	}
	return conn, nil
}

func (conn *ProxyConn) Run(ctx context.Context) error {
	defer conn.close()
	remoteAddr := conn.client.RemoteAddr()
	log.Printf("[debug][%s] start proxy connection", remoteAddr)
	defer log.Printf("[debug][%s] end proxy connection", remoteAddr)
	if _, err := conn.ExtendDeadline(); err != nil {
		return conn.wrapError(ctx, err, "extend initial deadline")
	}
	startupMessage, err := conn.backend.ReceiveStartupMessage()
	if err != nil {
		return conn.wrapError(ctx, err, "ReceiveStartupMessage")
	}
	log.Printf("[debug][%s] ReceiveStartupMessage", remoteAddr)
	switch startupMessage.(type) {
	case *pgproto3.SSLRequest:
		log.Printf("[debug][%s] SSLRequest", remoteAddr)
		if conn.opts.tlsConfig != nil {
			_, err := conn.client.Write([]byte("S"))
			if err != nil {
				return conn.wrapError(ctx, err, "send tls support")
			}
			log.Printf("[debug][%s] suport ssl", remoteAddr)
			tlsConn := tls.Server(conn.client, conn.opts.tlsConfig)
			conn.backend = pgproto3.NewBackend(pgproto3.NewChunkReader(tlsConn), tlsConn)
			conn.client = tlsConn
			startupMessage, err = conn.backend.ReceiveStartupMessage()
			if err != nil {
				return conn.wrapError(ctx, err, "ReceiveStartupMessage")
			}
		} else {
			log.Printf("[debug][%s] can not use ssl", remoteAddr)
			_, err := conn.client.Write([]byte("N"))
			if err != nil {
				return conn.wrapError(ctx, err, "send tls not support")
			}
		}
	case *pgproto3.GSSEncRequest:
		log.Printf("[debug][%s] can not use gss enc", remoteAddr)
		_, err := conn.client.Write([]byte("N"))
		if err != nil {
			return conn.wrapError(ctx, err, "send gss enc not support")
		}
	}
	if startupMessage, ok := startupMessage.(*pgproto3.StartupMessage); ok {
		var builder strings.Builder
		fmt.Fprintf(&builder, "protocol_version:%d", startupMessage.ProtocolVersion)
		for key, value := range startupMessage.Parameters {
			fmt.Fprintf(&builder, " %s:%s", key, value)
		}
		log.Printf("[info][%s] %s", remoteAddr, builder.String())
	}
	log.Printf("[debug][%s] send startup message to upstream", remoteAddr)
	if err := conn.frontend.Send(startupMessage); err != nil {
		return conn.wrapError(ctx, err, "frontend send startup message")
	}
	var cancelCtx context.Context
	cancelCtx, conn.cancel = context.WithCancel(ctx)
	eg, egCtx := errgroup.WithContext(cancelCtx)
	eg.Go(func() error {
		defer conn.cancel()
		for {
			select {
			case <-egCtx.Done():
				return nil
			default:
			}
			fm, err := conn.backend.Receive()
			if err != nil {
				return conn.wrapError(egCtx, err, "receive message from client")
			}
			if _, err := conn.ExtendDeadline(); err != nil {
				return conn.wrapError(egCtx, err, "failed extend deadline")
			}
			switch fm := fm.(type) {
			case *pgproto3.Query:
				log.Printf("[info][%s] receive message from client: incoming SQL: %s", remoteAddr, fm.String)
				if conn.opts.onQueryReceivedHandler != nil {
					if err := conn.opts.onQueryReceivedHandler(egCtx, fm.String, false, &notifier{backend: conn.backend}); err != nil {
						log.Printf("[error] on query received: %v", err)
						if err := conn.backend.Send(&pgproto3.ErrorResponse{
							Severity: "ERROR",
							Code:     "58030",
							Message:  "Failed on query received handler",
							Detail:   err.Error(),
						}); err != nil {
							return conn.wrapError(egCtx, err, "on query recived")
						}
					}
				}
			case *pgproto3.Parse:
				log.Printf("[info][%s] receive message from client: parse SQL: %s name=%s", remoteAddr, fm.Query, fm.Name)
				if conn.opts.onQueryReceivedHandler != nil {
					if err := conn.opts.onQueryReceivedHandler(egCtx, fm.Query, true, &notifier{backend: conn.backend}); err != nil {
						log.Printf("[error] on query received: %v", err)
						if err := conn.backend.Send(&pgproto3.ErrorResponse{
							Severity: "ERROR",
							Code:     "58030",
							Message:  "Failed on query received handler",
							Detail:   err.Error(),
						}); err != nil {
							return conn.wrapError(egCtx, err, "on query recived")
						}
					}
				}
			case *pgproto3.Describe:
				log.Printf("[debug][%s] receive message from client: describe: %s type='%c'", remoteAddr, fm.Name, fm.ObjectType)
			case *pgproto3.Bind:
				log.Printf("[debug][%s] receive message from client: bind: %s", remoteAddr, fm.PreparedStatement)
			case *pgproto3.Execute:
				log.Printf("[debug][%s] receive message from client: execute: %s max_rows=%d", remoteAddr, fm.Portal, fm.MaxRows)
			case *pgproto3.Terminate:
				log.Printf("[debug][%s] receive message from client: connection terminate", remoteAddr)
				if err := conn.frontend.Send(fm); err != nil {
					return conn.wrapError(egCtx, err, "send terminate message to upstream")
				}
				if err := conn.backend.Send(&pgproto3.CloseComplete{}); err != nil {
					return conn.wrapError(egCtx, err, "send close complete message to client")
				}
				return nil
			default:
				log.Printf("[debug][%s] receive message from client: %T", remoteAddr, fm)
			}
			err = conn.frontend.Send(fm)
			if err != nil {
				return conn.wrapError(egCtx, err, "send message to upstream")
			}
		}
	})
	eg.Go(func() error {
		defer conn.cancel()
		for {
			select {
			case <-egCtx.Done():
				return nil
			default:
			}
			bm, err := conn.frontend.Receive()
			if err != nil {
				return conn.wrapError(egCtx, err, "receive message from upstream")
			}
			conn.backend.SetAuthType(conn.frontend.GetAuthType())
			switch bm := bm.(type) {
			case *pgproto3.ParameterStatus:
				log.Printf("[debug][%s] set parameter status name=%s, value=%s", remoteAddr, bm.Name, bm.Value)
			case *pgproto3.CloseComplete:
				log.Printf("[debug][%s] close complete from upstream", remoteAddr)
				return nil
			default:
				log.Printf("[debug][%s] receive message from upstream: %T", remoteAddr, bm)
			}
			err = conn.backend.Send(bm)
			if err != nil {
				return conn.wrapError(egCtx, err, "send message to client")
			}
		}
	})
	eg.Go(func() error {
		<-egCtx.Done()
		conn.close()
		return nil
	})
	err = eg.Wait()
	conn.close()
	if err != nil {
		return err
	}
	return nil
}

func (conn *ProxyConn) SetIdleTimeout(idleTimeout time.Duration) {
	conn.idleTimeout = idleTimeout
}

func (conn *ProxyConn) ExtendDeadline() (time.Time, error) {
	if conn.idleTimeout == time.Duration(0) {
		return time.Time{}, nil
	}
	d := time.Now().Add(conn.idleTimeout)
	log.Printf("[debug][%s] extended deadline: %s", conn.client.RemoteAddr(), d)
	if err := conn.client.SetDeadline(d); err != nil {
		return d, err
	}
	return d, conn.upstream.SetDeadline(d)
}

func (conn *ProxyConn) wrapError(ctx context.Context, err error, msg string, args ...interface{}) error {
	select {
	case <-ctx.Done():
		log.Printf("[debug][%s] err but context.Done(): %v", conn.client.RemoteAddr(), err)
		return nil
	default:
		args = append(args, err)
		return fmt.Errorf(msg+":%w", args...)
	}
}

func (conn *ProxyConn) close() {
	if conn.isClosed {
		return
	}
	if conn.cancel != nil {
		conn.cancel()
	}
	if conn.client != nil {
		conn.client.Close()
	}
	if conn.upstream != nil {
		conn.frontend.Send(&pgproto3.Terminate{})
		conn.upstream.Close()
	}
	conn.isClosed = true
}
