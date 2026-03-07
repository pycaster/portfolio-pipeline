package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/vrmap/portfolio-pipeline/internal/pipeline"
	"github.com/vrmap/portfolio-pipeline/internal/store"
	"github.com/vrmap/portfolio-pipeline/migrations"
)

func main() {
	var (
		uploadsDir    = flag.String("uploads-dir", envOr("UPLOADS", "./uploads"), "Path to uploads directory")
		chAddr        = flag.String("clickhouse-addr", envOr("CH_ADDR", "localhost:9000"), "ClickHouse native TCP address")
		chDB          = flag.String("clickhouse-db", envOr("CH_DB", "portfolio"), "ClickHouse database")
		chUser        = flag.String("clickhouse-user", envOr("CH_USER", "default"), "ClickHouse user")
		chPassword    = flag.String("clickhouse-password", envOr("CH_PASS", ""), "ClickHouse password")
		migrateUp     = flag.Bool("migrate", false, "Apply pending migrations and exit")
		migrateStatus = flag.Bool("migrate-status", false, "Show migration status and exit")
	)
	flag.Parse()

	s, err := store.New(store.Config{
		Addr:     *chAddr,
		Database: *chDB,
		Username: *chUser,
		Password: *chPassword,
	})
	if err != nil {
		slog.Error("store connect", "err", err)
		os.Exit(1)
	}
	defer s.Close()

	ctx := context.Background()

	if *migrateStatus {
		if err := s.MigrateStatus(ctx, migrations.Files); err != nil {
			slog.Error("migrate status", "err", err)
			os.Exit(1)
		}
		return
	}

	if *migrateUp {
		if err := s.Migrate(ctx, migrations.Files); err != nil {
			slog.Error("migrate", "err", err)
			os.Exit(1)
		}
		return
	}

	p := pipeline.New(*uploadsDir, s)
	results, err := p.Run(ctx)
	if err != nil {
		slog.Error("pipeline", "err", err)
		os.Exit(1)
	}

	hasError := false

	for _, r := range results {
		switch {
		case r.Skipped && r.Error != nil:
			slog.Warn("skipped", "broker", r.Broker, "err", r.Error)
		case r.Skipped:
			// nothing new — silent
		case r.Error != nil:
			slog.Error("ingest failed", "broker", r.Broker, "file", r.File, "err", r.Error)
			hasError = true
		default:
			slog.Info("ingested", "broker", r.Broker, "file", r.File, "transactions", r.Transactions)
		}
	}

	if hasError {
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
