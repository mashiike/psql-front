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
	psqlfront "github.com/mashiike/psql-front"
	_ "github.com/mashiike/psql-front/origin/static"
)

var (
	Version = "current"
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
	flag.StringVar(&minLevel, "log-level", "info", "log level")
	flag.StringVar(&config, "config", "", "psql-front config")
	flag.Uint64Var(&port, "port", 5434, "psql-front port")
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
	if err := server.RunWithContext(ctx, fmt.Sprintf(":%d", port)); err != nil {
		log.Fatalf("[error] %v", err)
	}
}
