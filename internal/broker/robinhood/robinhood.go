package robinhood

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/vrmap/portfolio-pipeline/internal/broker"
)

// optionRe matches option instrument strings like "AAPL 1/17/2025 150.00C"
var optionRe = regexp.MustCompile(`^(\w+)\s+(\d{1,2}/\d{1,2}/\d{4})\s+([\d.]+)(C|P)$`)

// optionDescRe matches the Robinhood Description column for option trades:
// e.g. "NVDA 1/30/2026 Put $170.00" or "AAPL 2/20/2026 Call $200.00"
var optionDescRe = regexp.MustCompile(`^(\w+)\s+(\d{1,2}/\d{1,2}/\d{4})\s+(Call|Put)\s+\$([\d.]+)$`)

// Robinhood implements broker.Broker for Robinhood CSV exports.
//
// Expected CSV columns (tab or comma separated):
//
//	Activity Date, Process Date, Settle Date, Instrument, Description,
//	Trans Code, Quantity, Price, Amount
type Robinhood struct{}

func New() *Robinhood { return &Robinhood{} }

func (r *Robinhood) Name() string { return "robinhood" }

func (r *Robinhood) Parse(reader io.Reader, sourceFile string) ([]broker.Transaction, error) {
	cr := csv.NewReader(reader)
	cr.TrimLeadingSpace = true
	cr.LazyQuotes = true
	cr.FieldsPerRecord = -1 // allow rows with fewer fields (e.g. trailing empty rows)

	records, err := cr.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("reading CSV: %w", err)
	}
	if len(records) < 2 {
		return nil, fmt.Errorf("CSV has no data rows")
	}

	// Build column index from header row
	idx := make(map[string]int, len(records[0]))
	for i, h := range records[0] {
		idx[strings.TrimSpace(h)] = i
	}

	required := []string{"Activity Date", "Instrument", "Trans Code"}
	for _, col := range required {
		if _, ok := idx[col]; !ok {
			return nil, fmt.Errorf("missing required column %q — is this a Robinhood CSV?", col)
		}
	}

	get := func(row []string, col string) string {
		i, ok := idx[col]
		if !ok || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	var txns []broker.Transaction

	// seenKeys counts occurrences of each hash key so identical rows in the
	// same CSV (e.g. two separate fills for the same qty/price) get distinct hashes.
	seenKeys := make(map[string]int)

	for lineNum, row := range records[1:] {
		if allEmpty(row) {
			continue
		}

		actDate, err := parseDate(get(row, "Activity Date"))
		if err != nil {
			// Skip rows with no date (Robinhood footer/summary rows)
			if get(row, "Activity Date") == "" {
				continue
			}
			return nil, fmt.Errorf("line %d: invalid activity date %q: %w",
				lineNum+2, get(row, "Activity Date"), err)
		}

		// Use activity date as fallback for missing process/settle dates
		processDate, _ := parseDate(get(row, "Process Date"))
		if processDate.IsZero() {
			processDate = actDate
		}
		settleDate, _ := parseDate(get(row, "Settle Date"))
		if settleDate.IsZero() {
			settleDate = actDate
		}

		instrument := get(row, "Instrument")
		symbol, assetType, optExpiry, optStrike, optType := parseInstrument(instrument, get(row, "Description"))

		qty := parseFloat(get(row, "Quantity"))
		price := parseFloat(get(row, "Price"))
		amount := parseFloat(get(row, "Amount"))

		// Deterministic row hash for deduplication on re-import.
		// The occurrence counter disambiguates genuinely separate fills that
		// happen to share the same date/instrument/qty/price (e.g. two 453-share
		// USAR sells on the same day at the same price).
		baseKey := strings.Join([]string{
			"robinhood",
			get(row, "Activity Date"),
			instrument,
			get(row, "Trans Code"),
			get(row, "Quantity"),
			get(row, "Price"),
			get(row, "Amount"),
		}, "|")
		occurrence := seenKeys[baseKey]
		seenKeys[baseKey]++
		rawKey := fmt.Sprintf("%s|%d", baseKey, occurrence)
		h := sha256.Sum256([]byte(rawKey))
		rowHash := hex.EncodeToString(h[:16])

		txns = append(txns, broker.Transaction{
			RowHash:      rowHash,
			Broker:       "robinhood",
			ActivityDate: actDate,
			ProcessDate:  processDate,
			SettleDate:   settleDate,
			Instrument:   instrument,
			Symbol:       symbol,
			Description:  get(row, "Description"),
			TransCode:    strings.ToUpper(get(row, "Trans Code")),
			AssetType:    assetType,
			OptionExpiry: optExpiry,
			OptionStrike: optStrike,
			OptionType:   optType,
			Quantity:     qty,
			Price:        price,
			Amount:       amount,
			SourceFile:   sourceFile,
		})
	}

	return txns, nil
}

// parseInstrument detects whether a transaction is a stock or option.
// It first checks the instrument string (e.g. "AAPL 1/17/2025 150.00C"),
// then falls back to the description (e.g. "NVDA 1/30/2026 Put $170.00"),
// which is how Robinhood encodes option details when Instrument is just a ticker.
func parseInstrument(instrument, description string) (
	symbol, assetType string,
	optExpiry *time.Time,
	optStrike *float64,
	optType string,
) {
	s := strings.TrimSpace(instrument)
	if m := optionRe.FindStringSubmatch(s); m != nil {
		expiry, _ := time.Parse("1/2/2006", m[2])
		strike, _ := strconv.ParseFloat(m[3], 64)
		return strings.ToUpper(m[1]), "OPTION", &expiry, &strike, m[4]
	}
	if m := optionDescRe.FindStringSubmatch(strings.TrimSpace(description)); m != nil {
		expiry, _ := time.Parse("1/2/2006", m[2])
		strike, _ := strconv.ParseFloat(m[4], 64)
		optType := "C"
		if strings.EqualFold(m[3], "Put") {
			optType = "P"
		}
		return strings.ToUpper(m[1]), "OPTION", &expiry, &strike, optType
	}
	return strings.ToUpper(s), "STOCK", nil, nil, ""
}

func parseDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty date string")
	}
	// Robinhood uses M/D/YYYY
	if t, err := time.Parse("1/2/2006", s); err == nil {
		return t, nil
	}
	// ISO fallback
	return time.Parse("2006-01-02", s)
}

func parseFloat(s string) float64 {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, "$", "")
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func allEmpty(row []string) bool {
	for _, v := range row {
		if strings.TrimSpace(v) != "" {
			return false
		}
	}
	return true
}
