package robinhood_test

import (
	"strings"
	"testing"
	"time"

	"github.com/vrmap/portfolio-pipeline/internal/broker"
	"github.com/vrmap/portfolio-pipeline/internal/broker/robinhood"
)

// sampleCSV mirrors a real Robinhood export with stocks, options, and a dividend.
const sampleCSV = `Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
1/15/2026,1/15/2026,1/17/2026,AAPL,Apple Inc,BUY,10,185.20,-1852.00
1/20/2026,1/20/2026,1/22/2026,AAPL,Apple Inc,SELL,5,195.00,975.00
1/25/2026,1/25/2026,1/27/2026,NVDA,NVIDIA Corp,BUY,2,875.00,-1750.00
2/1/2026,2/1/2026,2/3/2026,AAPL 03/21/2026 200.00C,AAPL Call Option,BTO,1,3.50,-350.00
2/10/2026,2/10/2026,2/12/2026,AAPL 03/21/2026 200.00C,AAPL Call Option,STC,1,5.20,520.00
2/15/2026,2/15/2026,2/17/2026,TSLA 02/14/2026 300.00P,TSLA Put Option,OEXP,1,0.00,0.00
1/31/2026,1/31/2026,1/31/2026,AAPL,Apple Inc,DIV,,0.24,0.24

`

func parse(t *testing.T, csv string) []broker.Transaction {
	t.Helper()
	p := robinhood.New()
	txns, err := p.Parse(strings.NewReader(csv), "robinhood_test.csv")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	return txns
}

func TestParse_TransactionCount(t *testing.T) {
	txns := parse(t, sampleCSV)
	if len(txns) != 7 {
		t.Errorf("expected 7 transactions, got %d", len(txns))
	}
}

func TestParse_StockBuy(t *testing.T) {
	txns := parse(t, sampleCSV)
	tx := txns[0] // AAPL BUY

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"Broker", tx.Broker, "robinhood"},
		{"Symbol", tx.Symbol, "AAPL"},
		{"TransCode", tx.TransCode, "BUY"},
		{"AssetType", tx.AssetType, "STOCK"},
		{"Quantity", tx.Quantity, 10.0},
		{"Price", tx.Price, 185.20},
		{"Amount", tx.Amount, -1852.00},
		{"SourceFile", tx.SourceFile, "robinhood_test.csv"},
		{"OptionType", tx.OptionType, ""},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
	}
	if tx.OptionExpiry != nil {
		t.Errorf("OptionExpiry: expected nil for stock, got %v", tx.OptionExpiry)
	}
	if tx.OptionStrike != nil {
		t.Errorf("OptionStrike: expected nil for stock, got %v", tx.OptionStrike)
	}

	wantDate := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	if !tx.ActivityDate.Equal(wantDate) {
		t.Errorf("ActivityDate: got %v, want %v", tx.ActivityDate, wantDate)
	}
}

func TestParse_StockSell(t *testing.T) {
	txns := parse(t, sampleCSV)
	tx := txns[1] // AAPL SELL

	if tx.TransCode != "SELL" {
		t.Errorf("TransCode: got %q, want SELL", tx.TransCode)
	}
	if tx.Quantity != 5.0 {
		t.Errorf("Quantity: got %v, want 5.0", tx.Quantity)
	}
	if tx.Amount != 975.00 {
		t.Errorf("Amount: got %v, want 975.00", tx.Amount)
	}
}

func TestParse_OptionBTO(t *testing.T) {
	txns := parse(t, sampleCSV)
	tx := txns[3] // AAPL 03/21/2026 200.00C BTO

	if tx.AssetType != "OPTION" {
		t.Fatalf("AssetType: got %q, want OPTION", tx.AssetType)
	}
	if tx.Symbol != "AAPL" {
		t.Errorf("Symbol: got %q, want AAPL (underlying)", tx.Symbol)
	}
	if tx.TransCode != "BTO" {
		t.Errorf("TransCode: got %q, want BTO", tx.TransCode)
	}
	if tx.OptionType != "C" {
		t.Errorf("OptionType: got %q, want C", tx.OptionType)
	}
	if tx.OptionStrike == nil || *tx.OptionStrike != 200.00 {
		t.Errorf("OptionStrike: got %v, want 200.00", tx.OptionStrike)
	}

	wantExpiry := time.Date(2026, 3, 21, 0, 0, 0, 0, time.UTC)
	if tx.OptionExpiry == nil || !tx.OptionExpiry.Equal(wantExpiry) {
		t.Errorf("OptionExpiry: got %v, want %v", tx.OptionExpiry, wantExpiry)
	}
	// Instrument raw value must be preserved exactly
	if tx.Instrument != "AAPL 03/21/2026 200.00C" {
		t.Errorf("Instrument: got %q, want raw option string", tx.Instrument)
	}
}

func TestParse_OptionPut_OEXP(t *testing.T) {
	txns := parse(t, sampleCSV)
	tx := txns[5] // TSLA 02/14/2026 300.00P OEXP

	if tx.AssetType != "OPTION" {
		t.Fatalf("AssetType: got %q, want OPTION", tx.AssetType)
	}
	if tx.Symbol != "TSLA" {
		t.Errorf("Symbol: got %q, want TSLA", tx.Symbol)
	}
	if tx.TransCode != "OEXP" {
		t.Errorf("TransCode: got %q, want OEXP", tx.TransCode)
	}
	if tx.OptionType != "P" {
		t.Errorf("OptionType: got %q, want P", tx.OptionType)
	}
	if tx.OptionStrike == nil || *tx.OptionStrike != 300.00 {
		t.Errorf("OptionStrike: got %v, want 300.00", tx.OptionStrike)
	}
}

func TestParse_Dividend(t *testing.T) {
	txns := parse(t, sampleCSV)
	tx := txns[6] // AAPL DIV

	if tx.TransCode != "DIV" {
		t.Errorf("TransCode: got %q, want DIV", tx.TransCode)
	}
	if tx.AssetType != "STOCK" {
		t.Errorf("AssetType: got %q, want STOCK", tx.AssetType)
	}
	// Quantity is empty in CSV — should be zero, not an error
	if tx.Quantity != 0 {
		t.Errorf("Quantity: got %v, want 0 for dividend", tx.Quantity)
	}
	if tx.Amount != 0.24 {
		t.Errorf("Amount: got %v, want 0.24", tx.Amount)
	}
}

func TestParse_SettleDateFallsBackToActivityDate(t *testing.T) {
	// Settle date missing — should fall back to activity date
	csv := `Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
1/15/2026,1/15/2026,,AAPL,Apple Inc,BUY,10,185.20,-1852.00
`
	txns := parse(t, csv)
	if len(txns) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(txns))
	}
	wantDate := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	if !txns[0].SettleDate.Equal(wantDate) {
		t.Errorf("SettleDate fallback: got %v, want activity date %v", txns[0].SettleDate, wantDate)
	}
}

func TestParse_TrailingEmptyRowsIgnored(t *testing.T) {
	csv := `Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
1/15/2026,1/15/2026,1/17/2026,AAPL,Apple Inc,BUY,10,185.20,-1852.00
,,,,,,,
,,,,,,,
`
	txns := parse(t, csv)
	if len(txns) != 1 {
		t.Errorf("expected 1 transaction (empty rows skipped), got %d", len(txns))
	}
}

func TestParse_RowHashIsDeterministic(t *testing.T) {
	p := robinhood.New()
	txns1, _ := p.Parse(strings.NewReader(sampleCSV), "test.csv")
	txns2, _ := p.Parse(strings.NewReader(sampleCSV), "test.csv")

	for i := range txns1 {
		if txns1[i].RowHash != txns2[i].RowHash {
			t.Errorf("row %d: hash not deterministic — got %s then %s",
				i, txns1[i].RowHash, txns2[i].RowHash)
		}
	}
}

func TestParse_RowHashesAreUnique(t *testing.T) {
	txns := parse(t, sampleCSV)
	seen := make(map[string]int)
	for i, tx := range txns {
		if prev, ok := seen[tx.RowHash]; ok {
			t.Errorf("row %d has same hash as row %d: %s", i, prev, tx.RowHash)
		}
		seen[tx.RowHash] = i
	}
}

func TestParse_MissingRequiredColumn(t *testing.T) {
	// CSV missing "Trans Code" column
	bad := `Activity Date,Process Date,Settle Date,Instrument,Description,Quantity,Price,Amount
1/15/2026,1/15/2026,1/17/2026,AAPL,Apple Inc,10,185.20,-1852.00
`
	p := robinhood.New()
	_, err := p.Parse(strings.NewReader(bad), "bad.csv")
	if err == nil {
		t.Error("expected error for missing required column, got nil")
	}
}

func TestParse_DollarSignsAndCommasInPrice(t *testing.T) {
	csv := `Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
1/15/2026,1/15/2026,1/17/2026,BRK.B,Berkshire Hathaway,BUY,1,"$421,500.00","-$421,500.00"
`
	txns := parse(t, csv)
	if len(txns) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(txns))
	}
	if txns[0].Price != 421500.00 {
		t.Errorf("Price: got %v, want 421500.00 (dollar signs and commas stripped)", txns[0].Price)
	}
}
