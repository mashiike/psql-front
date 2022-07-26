package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/fatih/color"
	"github.com/fujiwara/logutils"
	"github.com/ken39arg/go-flagx"
	psqlfront "github.com/mashiike/psql-front"
	_ "github.com/mashiike/psql-front/origin/gdrive"
	_ "github.com/mashiike/psql-front/origin/http"
	_ "github.com/mashiike/psql-front/origin/static"
	"golang.org/x/sync/errgroup"
)

func main() {
	filter := &logutils.LevelFilter{
		Levels: []logutils.LogLevel{"debug", "info", "notice", "warn", "error"},
		ModifierFuncs: []logutils.ModifierFunc{
			logutils.Color(color.FgHiBlack),
			nil,
			logutils.Color(color.FgHiBlue),
			logutils.Color(color.FgYellow),
			logutils.Color(color.FgRed, color.BgBlack),
		},
		MinLevel: "info",
		Writer:   os.Stderr,
	}
	log.SetOutput(filter)
	var (
		minLevel string
		config   string
		port     uint64
	)
	flag.CommandLine.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "Usage of psql-front")
		fmt.Fprintln(flag.CommandLine.Output(), "version:", psqlfront.Version)
		flag.CommandLine.PrintDefaults()
	}
	flag.StringVar(&minLevel, "log-level", "info", "log level")
	flag.StringVar(&config, "config", "", "psql-front config")
	flag.Uint64Var(&port, "port", 5434, "psql-front port")

	var pc profConfig
	flag.BoolVar(&pc.enablePprof, "enable-pprof", false, "")
	flag.BoolVar(&pc.enableStats, "enable-stats", false, "")
	flag.IntVar(&pc.debugPort, "debug-port", 8080, "port to listen for debug")

	flag.VisitAll(flagx.EnvToFlagWithPrefix("PSQL_FRONT_"))
	flag.Parse()
	filter.SetMinLevel(logutils.LogLevel(strings.ToLower(minLevel)))

	cfg := psqlfront.DefaultConfig()
	if config != "" {
		if err := cfg.Load(config); err != nil {
			log.Fatalf("[error] %v", err)
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer cancel()
	server, err := psqlfront.New(ctx, cfg)
	if err != nil {
		log.Fatalf("[error] %v", err)
	}

	eg, egCtx := errgroup.WithContext(ctx)
	if pc.enabled() {
		eg.Go(func() error {
			return profiler(egCtx, &pc)
		})
	}
	eg.Go(func() error {
		return server.RunWithContext(egCtx, fmt.Sprintf(":%d", port))
	})
	if err := eg.Wait(); err != nil {
		log.Fatalf("[error] %v", err)
	}
}
