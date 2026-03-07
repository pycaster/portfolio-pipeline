package pipeline_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/vrmap/portfolio-pipeline/internal/broker"
	"github.com/vrmap/portfolio-pipeline/internal/pipeline"
)

// mockStore records what the pipeline tried to insert.
type mockStore struct {
	transactions []broker.Transaction
}

func (m *mockStore) InsertTransactions(_ context.Context, txns []broker.Transaction) (int, error) {
	m.transactions = append(m.transactions, txns...)
	return len(txns), nil
}

// robinhoodCSV is a minimal but realistic Robinhood export covering stocks and options.
const robinhoodCSV = `Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
1/15/2026,1/15/2026,1/17/2026,AAPL,Apple Inc,BUY,10,185.20,-1852.00
1/20/2026,1/20/2026,1/22/2026,AAPL,Apple Inc,SELL,5,195.00,975.00
1/25/2026,1/25/2026,1/27/2026,NVDA,NVIDIA Corp,BUY,2,875.00,-1750.00
2/1/2026,2/1/2026,2/3/2026,AAPL 03/21/2026 200.00C,AAPL Call Option,BTO,1,3.50,-350.00
2/15/2026,2/15/2026,2/17/2026,TSLA 02/14/2026 300.00P,TSLA Put Option,OEXP,1,0.00,0.00
`

// setupUploads creates a temp uploads directory with a broker subdirectory and CSV.
func setupUploads(t *testing.T, brokerName, csvContent string) string {
	t.Helper()
	uploadsDir := t.TempDir()
	brokerDir := filepath.Join(uploadsDir, brokerName)
	if err := os.MkdirAll(brokerDir, 0755); err != nil {
		t.Fatalf("creating broker dir: %v", err)
	}
	csvPath := filepath.Join(brokerDir, "2026-01-01_2026-02-27.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("writing CSV: %v", err)
	}
	return uploadsDir
}

func TestPipeline_IngestsNewCSV(t *testing.T) {
	uploadsDir := setupUploads(t, "robinhood", robinhoodCSV)
	store := &mockStore{}

	p := pipeline.New(uploadsDir, store)
	results, err := p.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]

	if r.Error != nil {
		t.Fatalf("result error: %v", r.Error)
	}
	if r.Skipped {
		t.Error("expected not skipped on first run")
	}
	if r.Broker != "robinhood" {
		t.Errorf("Broker: got %q, want robinhood", r.Broker)
	}
	if r.Transactions != 5 {
		t.Errorf("Transactions: got %d, want 5", r.Transactions)
	}
	if len(store.transactions) != 5 {
		t.Errorf("store received %d transactions, want 5", len(store.transactions))
	}
}

func TestPipeline_CorrectSymbolsIngested(t *testing.T) {
	uploadsDir := setupUploads(t, "robinhood", robinhoodCSV)
	store := &mockStore{}

	pipeline.New(uploadsDir, store).Run(context.Background()) //nolint:errcheck

	// Verify the symbols and asset types in the store
	type want struct{ symbol, assetType, transCode string }
	expected := []want{
		{"AAPL", "STOCK", "BUY"},
		{"AAPL", "STOCK", "SELL"},
		{"NVDA", "STOCK", "BUY"},
		{"AAPL", "OPTION", "BTO"},
		{"TSLA", "OPTION", "OEXP"},
	}
	for i, w := range expected {
		tx := store.transactions[i]
		if tx.Symbol != w.symbol {
			t.Errorf("row %d Symbol: got %q, want %q", i, tx.Symbol, w.symbol)
		}
		if tx.AssetType != w.assetType {
			t.Errorf("row %d AssetType: got %q, want %q", i, tx.AssetType, w.assetType)
		}
		if tx.TransCode != w.transCode {
			t.Errorf("row %d TransCode: got %q, want %q", i, tx.TransCode, w.transCode)
		}
	}
}


func TestPipeline_SkipsAlreadyIngestedFile(t *testing.T) {
	uploadsDir := setupUploads(t, "robinhood", robinhoodCSV)
	store := &mockStore{}
	p := pipeline.New(uploadsDir, store)

	// First run — should ingest
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatalf("first Run() error: %v", err)
	}

	// Second run without changing the file — should skip
	results, err := p.Run(context.Background())
	if err != nil {
		t.Fatalf("second Run() error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !results[0].Skipped {
		t.Error("expected Skipped=true on second run with unchanged file")
	}
	// Store should still only have 5 transactions from the first run
	if len(store.transactions) != 5 {
		t.Errorf("store has %d transactions after second run, want 5 (no duplicates)", len(store.transactions))
	}
}

func TestPipeline_IngestsAgainAfterFileUpdate(t *testing.T) {
	uploadsDir := setupUploads(t, "robinhood", robinhoodCSV)
	store := &mockStore{}
	p := pipeline.New(uploadsDir, store)

	// First run
	p.Run(context.Background()) //nolint:errcheck

	// Drop a new CSV (replacing the old one — later mtime)
	newCSV := robinhoodCSV + "2/20/2026,2/20/2026,2/24/2026,MSFT,Microsoft,BUY,3,420.00,-1260.00\n"
	csvPath := filepath.Join(uploadsDir, "robinhood", "2026-01-01_2026-02-27.csv")
	if err := os.WriteFile(csvPath, []byte(newCSV), 0644); err != nil {
		t.Fatalf("writing updated CSV: %v", err)
	}

	results, err := p.Run(context.Background())
	if err != nil {
		t.Fatalf("second Run() error: %v", err)
	}
	if results[0].Skipped {
		t.Error("expected not skipped after file update")
	}
	if results[0].Transactions != 6 {
		t.Errorf("Transactions: got %d, want 6 after file update", results[0].Transactions)
	}
}

func TestPipeline_UnknownBrokerFolderSkipped(t *testing.T) {
	uploadsDir := t.TempDir()
	// Create a folder with no registered parser
	os.MkdirAll(filepath.Join(uploadsDir, "some-unknown-broker"), 0755)
	os.WriteFile(filepath.Join(uploadsDir, "some-unknown-broker", "export.csv"), []byte("data"), 0644)

	store := &mockStore{}
	results, err := pipeline.New(uploadsDir, store).Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !results[0].Skipped {
		t.Error("unknown broker should be marked as skipped")
	}
	if results[0].Error == nil {
		t.Error("unknown broker should have a descriptive error")
	}
	if len(store.transactions) != 0 {
		t.Error("unknown broker should not insert any transactions")
	}
}

func TestPipeline_EmptyUploadsDir(t *testing.T) {
	uploadsDir := t.TempDir()
	store := &mockStore{}

	results, err := pipeline.New(uploadsDir, store).Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for empty uploads dir, got %d", len(results))
	}
}
