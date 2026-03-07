package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/vrmap/portfolio-pipeline/internal/broker"
	"github.com/vrmap/portfolio-pipeline/internal/broker/robinhood"
)

// brokerRegistry maps folder names (under uploads/) to their parser.
// Add new brokers here as they are implemented.
var brokerRegistry = map[string]broker.Broker{
	"robinhood": robinhood.New(),
}

// state tracks when each broker was last successfully ingested.
// Stored as JSON in uploads/.pipeline-state.json.
type state struct {
	LastIngest map[string]time.Time `json:"last_ingest"`
}

// Result is the outcome of processing one broker's latest CSV file.
type Result struct {
	Broker       string
	File         string
	Transactions int
	Skipped      bool  // true if no new file detected
	Error        error
}

// Storer is the subset of store.Store that the pipeline requires.
// Using an interface here keeps the pipeline testable without a real ClickHouse.
type Storer interface {
	InsertTransactions(ctx context.Context, txns []broker.Transaction) (int, error)
}

// Pipeline scans an uploads directory for broker subdirectories and ingests
// the latest CSV from each one into ClickHouse.
type Pipeline struct {
	uploadsDir string
	stateFile  string
	store      Storer
}

func New(uploadsDir string, s Storer) *Pipeline {
	return &Pipeline{
		uploadsDir: uploadsDir,
		stateFile:  filepath.Join(uploadsDir, ".pipeline-state.json"),
		store:      s,
	}
}

// Run scans uploadsDir for broker subdirectories, skips anything without a
// registered parser or without a newer CSV than the last ingest, and ingests
// the rest. Returns one Result per broker directory found.
func (p *Pipeline) Run(ctx context.Context) ([]Result, error) {
	st, err := p.loadState()
	if err != nil {
		return nil, fmt.Errorf("loading pipeline state: %w", err)
	}

	entries, err := os.ReadDir(p.uploadsDir)
	if err != nil {
		return nil, fmt.Errorf("reading uploads dir %q: %w", p.uploadsDir, err)
	}

	var results []Result

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		brokerName := entry.Name()

		b, ok := brokerRegistry[brokerName]
		if !ok {
			results = append(results, Result{
				Broker:  brokerName,
				Skipped: true,
				Error:   fmt.Errorf("no parser registered for broker %q", brokerName),
			})
			continue
		}

		csvPath, csvMtime, err := latestCSV(filepath.Join(p.uploadsDir, brokerName))
		if err != nil {
			results = append(results, Result{Broker: brokerName, Error: err})
			continue
		}
		if csvPath == "" {
			// No CSV files in this folder yet
			results = append(results, Result{Broker: brokerName, Skipped: true})
			continue
		}

		// Skip if the CSV hasn't changed since the last ingest
		if last, ok := st.LastIngest[brokerName]; ok && !csvMtime.After(last) {
			results = append(results, Result{
				Broker:  brokerName,
				File:    filepath.Base(csvPath),
				Skipped: true,
			})
			continue
		}

		r := p.ingestFile(ctx, b, csvPath)
		results = append(results, r)

		if r.Error == nil {
			st.LastIngest[brokerName] = time.Now()
		}
	}

	if err := p.saveState(st); err != nil {
		return results, fmt.Errorf("saving pipeline state: %w", err)
	}
	return results, nil
}

func (p *Pipeline) ingestFile(ctx context.Context, b broker.Broker, csvPath string) Result {
	r := Result{Broker: b.Name(), File: filepath.Base(csvPath)}

	f, err := os.Open(csvPath)
	if err != nil {
		r.Error = fmt.Errorf("opening file: %w", err)
		return r
	}
	defer f.Close()

	txns, err := b.Parse(f, filepath.Base(csvPath))
	if err != nil {
		r.Error = fmt.Errorf("parsing CSV: %w", err)
		return r
	}

	n, err := p.store.InsertTransactions(ctx, txns)
	if err != nil {
		r.Error = fmt.Errorf("inserting transactions: %w", err)
		return r
	}
	r.Transactions = n

	return r
}

// latestCSV returns the path and modification time of the most recently
// modified CSV file in dir, or ("", zero, nil) if none exist.
func latestCSV(dir string) (path string, mtime time.Time, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("reading %q: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".csv" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().After(mtime) {
			mtime = info.ModTime()
			path = filepath.Join(dir, e.Name())
		}
	}
	return path, mtime, nil
}

func (p *Pipeline) loadState() (state, error) {
	s := state{LastIngest: make(map[string]time.Time)}
	data, err := os.ReadFile(p.stateFile)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	return s, json.Unmarshal(data, &s)
}

func (p *Pipeline) saveState(s state) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p.stateFile, data, 0644)
}
