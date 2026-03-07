// email-ingest polls a Gmail inbox for Robinhood order-confirmation emails,
// parses each one deterministically, and inserts the transaction into ClickHouse.
// No AI is involved. Run this on a schedule (cron) — it is idempotent.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/vrmap/portfolio-pipeline/internal/broker"
	"github.com/vrmap/portfolio-pipeline/internal/broker/robinhood"
	"github.com/vrmap/portfolio-pipeline/internal/emailpoller"
	"github.com/vrmap/portfolio-pipeline/internal/store"
)

func main() {
	var (
		imapAddr  = flag.String("imap-addr", envOr("EMAIL_IMAP_ADDR", "imap.gmail.com:993"), "IMAP server address")
		emailUser = flag.String("email-user", envOr("EMAIL_USER", ""), "Email address")
		emailPass = flag.String("email-pass", envOr("MAIL_APP_PASSWORD", ""), "App password")
		folder    = flag.String("email-folder", envOr("EMAIL_FOLDER", "INBOX"), "IMAP folder to poll")
		sender    = flag.String("email-sender", envOr("EMAIL_SENDER", ""), "Filter: only process emails from this address")
		chAddr    = flag.String("clickhouse-addr", envOr("CH_ADDR", "localhost:9000"), "ClickHouse native TCP address")
		chDB      = flag.String("clickhouse-db", envOr("CH_DB", "portfolio"), "ClickHouse database")
		chUser    = flag.String("clickhouse-user", envOr("CH_USER", "default"), "ClickHouse user")
		chPass    = flag.String("clickhouse-password", envOr("CH_PASS", ""), "ClickHouse password")
	)
	flag.Parse()

	if *emailUser == "" {
		slog.Error("EMAIL_USER is required")
		os.Exit(1)
	}
	if *emailPass == "" {
		slog.Error("MAIL_APP_PASSWORD is required")
		os.Exit(1)
	}
	if *sender == "" {
		slog.Error("EMAIL_SENDER is required")
		os.Exit(1)
	}

	// ── Fetch unseen emails ───────────────────────────────────────────────────
	poller := emailpoller.New(emailpoller.Config{
		Addr:     *imapAddr,
		User:     *emailUser,
		Password: *emailPass,
		Folder:   *folder,
		Sender:   *sender,
	})

	msgs, err := poller.FetchUnseen()
	if err != nil {
		slog.Error("fetch emails", "err", err)
		os.Exit(1)
	}
	if len(msgs) == 0 {
		slog.Info("no new emails")
		return
	}

	// ── Parse each email → Transaction ───────────────────────────────────────
	sourceFile := fmt.Sprintf("email:robinhood:%s", time.Now().UTC().Format("2006-01-02"))
	var txns []broker.Transaction

	for _, msg := range msgs {
		txn, err := robinhood.ParseEmail(msg.Body, sourceFile)
		if err != nil {
			slog.Warn("parse failed", "uid", msg.UID, "err", err)
			dumpFile := fmt.Sprintf("/tmp/robinhood-email-%d.txt", msg.UID)
			if werr := os.WriteFile(dumpFile, []byte(msg.Body), 0600); werr == nil {
				slog.Info("body dumped", "file", dumpFile)
			}
			continue
		}
		if txn == nil {
			slog.Debug("skipped non-trade email", "uid", msg.UID)
			continue
		}
		slog.Info("parsed", "uid", msg.UID,
			"date", txn.ActivityDate.Format("2006-01-02"),
			"code", txn.TransCode, "symbol", txn.Symbol,
			"qty", txn.Quantity, "price", txn.Price)
		txns = append(txns, *txn)
	}

	if len(txns) == 0 {
		slog.Error("no transactions parsed")
		os.Exit(1)
	}

	// ── Insert into ClickHouse ────────────────────────────────────────────────
	s, err := store.New(store.Config{
		Addr:     *chAddr,
		Database: *chDB,
		Username: *chUser,
		Password: *chPass,
	})
	if err != nil {
		slog.Error("clickhouse connect", "err", err)
		os.Exit(1)
	}
	defer s.Close()

	n, err := s.InsertTransactions(context.Background(), txns)
	if err != nil {
		slog.Error("insert", "err", err)
		os.Exit(1)
	}

	slog.Info("inserted", "count", n)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
