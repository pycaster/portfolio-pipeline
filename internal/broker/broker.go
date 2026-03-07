package broker

import (
	"io"
	"time"
)

// Transaction is the canonical representation of a single trading event,
// normalized from any broker's CSV format.
type Transaction struct {
	RowHash      string     // SHA-256 of raw row fields — used for deduplication
	Broker       string
	ActivityDate time.Time
	ProcessDate  time.Time
	SettleDate   time.Time
	Instrument   string     // raw value from CSV (option symbol or stock ticker)
	Symbol       string     // underlying symbol, always uppercase (e.g. "AAPL")
	Description  string
	TransCode    string     // BUY, SELL, STO, BTC, BTO, STC, OEXP, OASGN, DIV, ...
	AssetType    string     // "STOCK" or "OPTION"
	OptionExpiry *time.Time // nil for stocks
	OptionStrike *float64   // nil for stocks
	OptionType   string     // "C", "P", or "" for stocks
	Quantity     float64
	Price        float64
	Amount       float64
	SourceFile   string
}

// Broker parses a single broker's CSV export into canonical Transactions.
type Broker interface {
	Name() string
	Parse(r io.Reader, sourceFile string) ([]Transaction, error)
}
