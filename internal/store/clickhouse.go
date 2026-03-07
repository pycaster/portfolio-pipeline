package store

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/vrmap/portfolio-pipeline/internal/broker"
)

// Config holds ClickHouse connection parameters.
type Config struct {
	Addr     string // host:port for native TCP (default: localhost:9000)
	Database string
	Username string
	Password string
}

// Store wraps a ClickHouse connection with portfolio-specific insert methods.
type Store struct {
	conn clickhouse.Conn
}

func New(cfg Config) (*Store, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:  10 * time.Second,
		MaxOpenConns: 4,
	})
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("ping failed — is ClickHouse running at %s? %w", cfg.Addr, err)
	}
	return &Store{conn: conn}, nil
}

func (s *Store) Close() error { return s.conn.Close() }

// InsertTransactions batch-inserts transactions into portfolio.transactions.
// Duplicate row_hash values are silently collapsed by ReplacingMergeTree.
func (s *Store) InsertTransactions(ctx context.Context, txns []broker.Transaction) (int, error) {
	if len(txns) == 0 {
		return 0, nil
	}

	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO portfolio.transactions (
		row_hash, broker, activity_date, process_date, settle_date,
		instrument, symbol, description, trans_code, asset_type,
		option_expiry, option_strike, option_type,
		quantity, price, amount, source_file
	)`)
	if err != nil {
		return 0, fmt.Errorf("preparing transaction batch: %w", err)
	}

	for _, t := range txns {
		if err := batch.Append(
			t.RowHash,
			t.Broker,
			t.ActivityDate,
			t.ProcessDate,
			t.SettleDate,
			t.Instrument,
			t.Symbol,
			t.Description,
			t.TransCode,
			t.AssetType,
			t.OptionExpiry,
			t.OptionStrike,
			t.OptionType,
			t.Quantity,
			t.Price,
			t.Amount,
			t.SourceFile,
		); err != nil {
			return 0, fmt.Errorf("appending transaction %s: %w", t.RowHash, err)
		}
	}

	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("sending transaction batch: %w", err)
	}
	return len(txns), nil
}

// InsertPricesFromTransactions writes stock trade prices into portfolio.prices
// as provisional data (source='transaction'). Options are skipped — their
// Price field is the option premium, not the underlying stock price.
// Full OHLCV data from yfinance will supersede these rows via ReplacingMergeTree.
func (s *Store) InsertPricesFromTransactions(ctx context.Context, txns []broker.Transaction) (int, error) {
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO portfolio.prices
		(symbol, date, open, high, low, close, volume, source)`)
	if err != nil {
		return 0, fmt.Errorf("preparing prices batch: %w", err)
	}

	// Deduplicate: one price entry per symbol per day
	seen := make(map[string]bool)
	count := 0

	for _, t := range txns {
		if t.AssetType != "STOCK" || t.Price == 0 {
			continue
		}
		key := t.Symbol + "|" + t.ActivityDate.Format("2006-01-02")
		if seen[key] {
			continue
		}
		seen[key] = true

		// We only have trade price — set all OHLC fields to it.
		// yfinance ingest will replace this with real OHLCV later.
		if err := batch.Append(
			t.Symbol,
			t.ActivityDate,
			t.Price, // open
			t.Price, // high
			t.Price, // low
			t.Price, // close
			uint64(0),
			"transaction",
		); err != nil {
			return 0, fmt.Errorf("appending price for %s: %w", t.Symbol, err)
		}
		count++
	}

	if count == 0 {
		return 0, nil
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("sending prices batch: %w", err)
	}
	return count, nil
}
