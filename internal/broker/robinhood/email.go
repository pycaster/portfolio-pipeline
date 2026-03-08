package robinhood

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/vrmap/portfolio-pipeline/internal/broker"
)

// Robinhood order confirmation email patterns.
// The email is HTML-only. The order fields appear as:
//
//	<b>Symbol</b>: SOL<br>
//	<b>Type: </b>Market buy<br>
//	<b>Amount filled: </b>100 SOL at $88.05<br>
//	<b>Filled notional value: </b>$8,804.91<br>
//	<b>Date filled: </b>February 28, 2026 at 9:16 PM ET<br>
var (
	// Order URL contains type=currency for crypto, type=equity for stocks.
	emailOrderTypeRe = regexp.MustCompile(`(?i)applink\.robinhood\.com/orders\?[^"]*[&?]type=(\w+)`)

	// Spread full fill: "limit order to open 100 SOFI Call Debit Spreads executed at an average price of $265.00 for a total of $26,500.00 on March 6, 2026"
	emailSpreadRe = regexp.MustCompile(`(?i)limit order to (open|close) ([\d,]+) ([A-Z]+) [A-Za-z\s]+?Spread[s]? executed at an average price of \$([\d,]+(?:\.\d+)?) for a total of \$([\d,]+(?:\.\d+)?) on (\w+ \d{1,2}, \d{4})`)
	// Spread partial fill: "limit order to close 25 MSFT Call Debit Spreads executed on March 3, 2026 ... So far, 8 of 25 of your order was filled for an average price of $465.00 each."
	emailSpreadPartialRe = regexp.MustCompile(`(?i)limit order to (open|close) [\d,]+ ([A-Z]+) [A-Za-z\s]+?Spread[s]? executed on (\w+ \d{1,2}, \d{4})[^.]*\.\s*So far,\s+([\d,]+) of [\d,]+[^$]+\$([\d,]+(?:\.\d+)?) each`)

	// Option full fill: "limit order to sell 25 contracts of IREN $47.50 Call 3/20 ... executed at an average price of $176.00 per contract on March 2, 2026"
	emailOptionRe      = regexp.MustCompile(`(?i)limit order to (buy|sell)\s+([\d,]+)\s+contracts? of\s+([A-Z]+)\s+\$?([\d,]+(?:\.\d+)?)\s+(Call|Put)\s+(\d{1,2}/\d{1,2})`)
	emailOptionPriceRe = regexp.MustCompile(`(?i)average price of \$([\d,]+(?:\.\d+)?)\s+per contract on (\w+\s+\d{1,2},\s+\d{4})`)

	// Option partial fill: "So far, 6 of 60 contracts were filled for an average price of $181.00 per contract"
	emailOptionPartialRe   = regexp.MustCompile(`(?i)So far,\s+([\d,]+)\s+of\s+[\d,]+\s+contracts? were filled for an average price of \$([\d,]+(?:\.\d+)?)\s+per contract`)
	emailOptionExecDateRe  = regexp.MustCompile(`(?i)executed\s+(?:at\s+[^.]*\s+)?on\s+(\w+\s+\d{1,2},\s+\d{4})`)

	// Stock fill: "order to sell 600 shares of NVDA ... was executed at an average price of $179.33 on March 2, 2026"
	emailStockRe = regexp.MustCompile(`(?i)order to (buy|sell)\s+([\d,]+)\s+shares? of\s+([A-Z]+)[^.]*?was executed at an average price of \$([\d,]+(?:\.\d+)?)\s+on (\w+\s+\d{1,2},\s+\d{4})`)

	// Crypto fill: "market order to buy 100 SOL was filled for $8,804.91"
	// Qty uses [\d,]+(?:\.\d+)? to handle fractional amounts like 0.00031 BTC.
	emailCryptoRe = regexp.MustCompile(`(?i)(market|limit) order to (buy|sell)\s+([\d,]+(?:\.\d+)?)\s+([A-Z]+)\s+was filled for \$([\d,]+(?:\.\d+)?)`)

	// Legacy structured format (pre-2026 Robinhood emails).
	emailSymbolRe   = regexp.MustCompile(`(?i)<b>Symbol</b>:\s*([A-Z0-9./]+)`)
	emailTypeRe     = regexp.MustCompile(`(?i)<b>Type:\s*</b>([^<]+)`)
	emailDateRe     = regexp.MustCompile(`(?i)<b>Date filled:\s*</b>(\w+ \d{1,2}, \d{4})`)
	emailFillRe     = regexp.MustCompile(`(?i)<b>Amount filled:\s*</b>([\d,]+(?:\.\d+)?)\s+\S+\s+at\s+\$([\d,]+(?:\.\d+)?)`)
	emailNotionalRe = regexp.MustCompile(`(?i)<b>Filled notional value:\s*</b>\$([\d,]+(?:\.\d+)?)`)

	// Title extraction for skip detection.
	emailTitleRe = regexp.MustCompile(`(?i)font-size:24px;[^>]+>([^<]+)<`)
)

// ParseEmail parses a Robinhood order-filled email body into a Transaction.
// Returns nil, nil for non-trade emails (canceled, statements, etc.) that
// should be silently skipped. Returns an error only for emails that look like
// trades but cannot be parsed.
func ParseEmail(body, sourceFile string) (*broker.Transaction, error) {
	// Skip non-trade emails (canceled, replaced, statements, etc.).
	if isNonTradeEmail(body) {
		return nil, nil
	}

	// Try each prose format in order.
	if emailSpreadRe.MatchString(body) || emailSpreadPartialRe.MatchString(body) {
		return parseSpreadEmail(body, sourceFile)
	}
	if emailOptionRe.MatchString(body) {
		return parseOptionProseEmail(body, sourceFile)
	}
	if emailStockRe.MatchString(body) {
		return parseStockProseEmail(body, sourceFile)
	}
	if emailCryptoRe.MatchString(body) {
		return parseCryptoProseEmail(body, sourceFile)
	}

	// Legacy structured format (pre-2026 Robinhood emails).
	return parseLegacyEmail(body, sourceFile)
}

// isNonTradeEmail returns true for emails that don't represent executed trades.
func isNonTradeEmail(body string) bool {
	m := emailTitleRe.FindStringSubmatch(body)
	if m == nil {
		// No recognizable title → marketing/notification email, skip.
		return true
	}
	title := strings.ToLower(strings.TrimSpace(m[1]))
	if title == "" {
		return true
	}
	// "details" intentionally excluded — Robinhood Crypto fills are titled "Your BTC order details"
	skipWords := []string{"cancel", "replac", "statement", "confirmation", "available", "futures", "dividend", "upcoming", "going"}
	for _, word := range skipWords {
		if strings.Contains(title, word) {
			return true
		}
	}
	return false
}

// parseSpreadEmail handles Robinhood spread order emails which use a prose
// format: "limit order to open 100 SOFI Call Debit Spreads executed at an
// average price of $265.00 for a total of $26,500.00 on March 6, 2026..."
func parseSpreadEmail(body, sourceFile string) (*broker.Transaction, error) {
	var action, symbol string
	var qty, pricePerContract, total float64
	var actDate time.Time

	if m := emailSpreadRe.FindStringSubmatch(body); m != nil {
		// Full fill: m[1]=open|close, m[2]=qty, m[3]=symbol, m[4]=price, m[5]=total, m[6]=date
		action = strings.ToLower(m[1])
		symbol = strings.ToUpper(strings.TrimSpace(m[3]))
		var err error
		qty, err = parseEmailFloat(m[2])
		if err != nil {
			return nil, fmt.Errorf("spread email: parse qty: %w", err)
		}
		pricePerContract, err = parseEmailFloat(m[4])
		if err != nil {
			return nil, fmt.Errorf("spread email: parse price: %w", err)
		}
		total, err = parseEmailFloat(m[5])
		if err != nil {
			return nil, fmt.Errorf("spread email: parse total: %w", err)
		}
		actDate, err = time.Parse("January 2, 2006", m[6])
		if err != nil {
			return nil, fmt.Errorf("spread email: parse date %q: %w", m[6], err)
		}
	} else if m := emailSpreadPartialRe.FindStringSubmatch(body); m != nil {
		// Partial fill: m[1]=open|close, m[2]=symbol, m[3]=date, m[4]=filled_qty, m[5]=price_each
		action = strings.ToLower(m[1])
		symbol = strings.ToUpper(strings.TrimSpace(m[2]))
		var err error
		actDate, err = time.Parse("January 2, 2006", m[3])
		if err != nil {
			return nil, fmt.Errorf("spread partial: parse date %q: %w", m[3], err)
		}
		qty, err = parseEmailFloat(m[4])
		if err != nil {
			return nil, fmt.Errorf("spread partial: parse qty: %w", err)
		}
		pricePerContract, err = parseEmailFloat(m[5])
		if err != nil {
			return nil, fmt.Errorf("spread partial: parse price: %w", err)
		}
		total = qty * pricePerContract
	} else {
		return nil, fmt.Errorf("spread email: no pattern matched")
	}

	transCode := "BTO"
	if action == "close" {
		transCode = "STC"
	}

	// Email reports price per spread contract; normalize to per-share (÷100)
	// to match all other option transactions stored in portfolio.transactions.
	price := pricePerContract / 100.0

	// Amount: 0 for opens (debit); total credit for closes.
	var amount float64
	if transCode == "STC" {
		amount = total
	}

	rowHash := emailRowHash(actDate, symbol, transCode, qty, price)
	return &broker.Transaction{
		RowHash:      rowHash,
		Broker:       "robinhood",
		ActivityDate: actDate,
		ProcessDate:  actDate,
		SettleDate:   actDate,
		Instrument:   symbol,
		Symbol:       symbol,
		TransCode:    transCode,
		AssetType:    "OPTION",
		Quantity:     qty,
		Price:        price,
		Amount:       amount,
		SourceFile:   sourceFile,
	}, nil
}

// parseOptionProseEmail handles the new Robinhood option email format:
// "limit order to sell 25 contracts of IREN $47.50 Call 3/20 ... executed at
//  an average price of $176.00 per contract on March 2, 2026"
// Also handles partial fills: "So far, 6 of 60 contracts were filled for ..."
func parseOptionProseEmail(body, sourceFile string) (*broker.Transaction, error) {
	hdr := emailOptionRe.FindStringSubmatch(body)
	if hdr == nil {
		return nil, fmt.Errorf("option prose: header not matched")
	}
	// hdr[1]=buy|sell, hdr[2]=total_qty, hdr[3]=symbol, hdr[4]=strike, hdr[5]=Call|Put, hdr[6]=MM/DD

	direction := strings.ToLower(hdr[1])
	transCode := "BTO"
	if direction == "sell" {
		transCode = "STC"
	}

	symbol := strings.ToUpper(hdr[3])
	strike, err := parseEmailFloat(strings.ReplaceAll(hdr[4], ",", ""))
	if err != nil {
		return nil, fmt.Errorf("option prose: parse strike: %w", err)
	}
	optType := "C"
	if strings.ToLower(hdr[5]) == "put" {
		optType = "P"
	}

	// Partial fill takes filled qty; full fill uses header qty.
	var qty, price float64
	var actDate time.Time

	if pm := emailOptionPartialRe.FindStringSubmatch(body); pm != nil {
		// pm[1]=filled_qty, pm[2]=price_per_contract
		qty, err = parseEmailFloat(pm[1])
		if err != nil {
			return nil, fmt.Errorf("option partial: parse qty: %w", err)
		}
		price, err = parseEmailFloat(pm[2])
		if err != nil {
			return nil, fmt.Errorf("option partial: parse price: %w", err)
		}
		dm := emailOptionExecDateRe.FindStringSubmatch(body)
		if dm == nil {
			return nil, fmt.Errorf("option partial: date not found")
		}
		actDate, err = time.Parse("January 2, 2006", strings.TrimSpace(dm[1]))
		if err != nil {
			return nil, fmt.Errorf("option partial: parse date: %w", err)
		}
	} else {
		pm2 := emailOptionPriceRe.FindStringSubmatch(body)
		if pm2 == nil {
			return nil, fmt.Errorf("option prose: price/date not found")
		}
		// pm2[1]=price_per_contract, pm2[2]=date
		price, err = parseEmailFloat(pm2[1])
		if err != nil {
			return nil, fmt.Errorf("option prose: parse price: %w", err)
		}
		actDate, err = time.Parse("January 2, 2006", strings.TrimSpace(pm2[2]))
		if err != nil {
			return nil, fmt.Errorf("option prose: parse date: %w", err)
		}
		qty, err = parseEmailFloat(hdr[2])
		if err != nil {
			return nil, fmt.Errorf("option prose: parse qty: %w", err)
		}
	}

	// Email reports price per contract (×100 shares); normalize to per-share.
	price = price / 100.0

	// Parse expiry MM/DD, infer year from execution date.
	expiry, err := parseOptionExpiry(hdr[6], actDate)
	if err != nil {
		return nil, fmt.Errorf("option prose: parse expiry: %w", err)
	}

	instrument := fmt.Sprintf("%s %s %.2f%s", symbol, expiry.Format("01/02/2006"), strike, optType)

	// Amount: 0 for BTO; notional for STC (qty × price_per_share × 100).
	var amount float64
	if transCode == "STC" {
		amount = qty * price * 100
	}

	rowHash := emailRowHash(actDate, symbol, transCode, qty, price)
	return &broker.Transaction{
		RowHash:      rowHash,
		Broker:       "robinhood",
		ActivityDate: actDate,
		ProcessDate:  actDate,
		SettleDate:   actDate,
		Instrument:   instrument,
		Symbol:       symbol,
		TransCode:    transCode,
		AssetType:    "OPTION",
		Quantity:     qty,
		Price:        price,
		Amount:       amount,
		OptionType:   optType,
		OptionStrike: &strike,
		OptionExpiry: &expiry,
		SourceFile:   sourceFile,
	}, nil
}

// parseStockProseEmail handles: "order to sell 600 shares of NVDA ... was
// executed at an average price of $179.33 on March 2, 2026"
func parseStockProseEmail(body, sourceFile string) (*broker.Transaction, error) {
	m := emailStockRe.FindStringSubmatch(body)
	if m == nil {
		return nil, fmt.Errorf("stock prose: pattern not matched")
	}
	// m[1]=buy|sell, m[2]=qty, m[3]=symbol, m[4]=price, m[5]=date

	direction := strings.ToLower(m[1])
	transCode := "BUY"
	if direction == "sell" {
		transCode = "SELL"
	}

	qty, err := parseEmailFloat(m[2])
	if err != nil {
		return nil, fmt.Errorf("stock prose: parse qty: %w", err)
	}
	price, err := parseEmailFloat(m[4])
	if err != nil {
		return nil, fmt.Errorf("stock prose: parse price: %w", err)
	}
	actDate, err := time.Parse("January 2, 2006", strings.TrimSpace(m[5]))
	if err != nil {
		return nil, fmt.Errorf("stock prose: parse date: %w", err)
	}

	assetType := "STOCK"
	if om := emailOrderTypeRe.FindStringSubmatch(body); om != nil && strings.ToLower(om[1]) == "currency" {
		assetType = "CRYPTO"
	}

	var amount float64
	if transCode == "SELL" {
		amount = qty * price
	}

	rowHash := emailRowHash(actDate, m[3], transCode, qty, price)
	return &broker.Transaction{
		RowHash:      rowHash,
		Broker:       "robinhood",
		ActivityDate: actDate,
		ProcessDate:  actDate,
		SettleDate:   actDate,
		Instrument:   strings.ToUpper(m[3]),
		Symbol:       strings.ToUpper(m[3]),
		TransCode:    transCode,
		AssetType:    assetType,
		Quantity:     qty,
		Price:        price,
		Amount:       amount,
		SourceFile:   sourceFile,
	}, nil
}

// parseCryptoProseEmail handles: "market order to buy 100 SOL was filled for $8,804.91"
// No per-unit price in the email — compute from total / qty.
func parseCryptoProseEmail(body, sourceFile string) (*broker.Transaction, error) {
	m := emailCryptoRe.FindStringSubmatch(body)
	if m == nil {
		return nil, fmt.Errorf("crypto prose: pattern not matched")
	}
	// m[1]=market|limit, m[2]=buy|sell, m[3]=qty, m[4]=symbol, m[5]=total

	direction := strings.ToLower(m[2])
	transCode := "BUY"
	if direction == "sell" {
		transCode = "SELL"
	}

	qty, err := parseEmailFloat(m[3])
	if err != nil {
		return nil, fmt.Errorf("crypto prose: parse qty: %w", err)
	}
	total, err := parseEmailFloat(m[5])
	if err != nil {
		return nil, fmt.Errorf("crypto prose: parse total: %w", err)
	}
	if qty == 0 {
		return nil, fmt.Errorf("crypto prose: qty is zero")
	}
	price := total / qty
	symbol := strings.ToUpper(m[4])

	// Crypto emails don't include a date in the prose — use today.
	actDate := time.Now().UTC().Truncate(24 * time.Hour)

	var amount float64
	if transCode == "SELL" {
		amount = total
	}

	rowHash := emailRowHash(actDate, symbol, transCode, qty, price)
	return &broker.Transaction{
		RowHash:      rowHash,
		Broker:       "robinhood",
		ActivityDate: actDate,
		ProcessDate:  actDate,
		SettleDate:   actDate,
		Instrument:   symbol,
		Symbol:       symbol,
		TransCode:    transCode,
		AssetType:    "CRYPTO",
		Quantity:     qty,
		Price:        price,
		Amount:       amount,
		SourceFile:   sourceFile,
	}, nil
}

// parseLegacyEmail handles pre-2026 Robinhood structured emails with
// <b>Symbol</b>: / <b>Type:</b> / etc. tags.
func parseLegacyEmail(body, sourceFile string) (*broker.Transaction, error) {
	m := emailSymbolRe.FindStringSubmatch(body)
	if m == nil {
		return nil, fmt.Errorf("symbol not found")
	}
	symbol := strings.ToUpper(strings.TrimSpace(m[1]))

	m = emailTypeRe.FindStringSubmatch(body)
	if m == nil {
		return nil, fmt.Errorf("order type not found")
	}
	transCode, err := emailTransCode(strings.TrimSpace(m[1]))
	if err != nil {
		return nil, err
	}

	m = emailDateRe.FindStringSubmatch(body)
	if m == nil {
		return nil, fmt.Errorf("date filled not found")
	}
	actDate, err := time.Parse("January 2, 2006", m[1])
	if err != nil {
		return nil, fmt.Errorf("parse date %q: %w", m[1], err)
	}

	m = emailFillRe.FindStringSubmatch(body)
	if m == nil {
		return nil, fmt.Errorf("amount filled not found")
	}
	qty, err := parseEmailFloat(m[1])
	if err != nil {
		return nil, fmt.Errorf("parse quantity: %w", err)
	}
	price, err := parseEmailFloat(m[2])
	if err != nil {
		return nil, fmt.Errorf("parse price: %w", err)
	}

	var amount float64
	if transCode == "SELL" || transCode == "STC" || transCode == "STO" {
		m = emailNotionalRe.FindStringSubmatch(body)
		if m == nil {
			return nil, fmt.Errorf("filled notional value not found for sell order")
		}
		amount, err = parseEmailFloat(m[1])
		if err != nil {
			return nil, fmt.Errorf("parse notional: %w", err)
		}
	}

	assetType := "STOCK"
	if m = emailOrderTypeRe.FindStringSubmatch(body); m != nil && strings.ToLower(m[1]) == "currency" {
		assetType = "CRYPTO"
	}

	rowHash := emailRowHash(actDate, symbol, transCode, qty, price)
	return &broker.Transaction{
		RowHash:      rowHash,
		Broker:       "robinhood",
		ActivityDate: actDate,
		ProcessDate:  actDate,
		SettleDate:   actDate,
		Instrument:   symbol,
		Symbol:       symbol,
		TransCode:    transCode,
		AssetType:    assetType,
		Quantity:     qty,
		Price:        price,
		Amount:       amount,
		SourceFile:   sourceFile,
	}, nil
}

// parseOptionExpiry parses "MM/DD" and infers the year from execDate.
func parseOptionExpiry(mmdd string, execDate time.Time) (time.Time, error) {
	parts := strings.Split(mmdd, "/")
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("unexpected expiry format %q", mmdd)
	}
	month, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Time{}, err
	}
	day, err := strconv.Atoi(parts[1])
	if err != nil {
		return time.Time{}, err
	}
	year := execDate.Year()
	candidate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if candidate.Before(execDate) {
		candidate = time.Date(year+1, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	}
	return candidate, nil
}

// emailTransCode maps Robinhood order type strings to canonical trans codes.
func emailTransCode(typeStr string) (string, error) {
	lower := strings.ToLower(typeStr)
	switch {
	case strings.Contains(lower, "buy to open"):
		return "BTO", nil
	case strings.Contains(lower, "buy to close"):
		return "BTC", nil
	case strings.Contains(lower, "sell to open"):
		return "STO", nil
	case strings.Contains(lower, "sell to close"):
		return "STC", nil
	case strings.Contains(lower, "buy"):
		return "BUY", nil
	case strings.Contains(lower, "sell"):
		return "SELL", nil
	default:
		return "", fmt.Errorf("unrecognized order type %q", typeStr)
	}
}

// emailRowHash produces a deterministic dedup key for email-sourced transactions.
// Uses the "email:robinhood" prefix so it never collides with CSV-sourced hashes.
func emailRowHash(date time.Time, symbol, transCode string, qty, price float64) string {
	key := strings.Join([]string{
		"email:robinhood",
		date.Format("1/2/2006"),
		symbol,
		transCode,
		strconv.FormatFloat(qty, 'f', -1, 64),
		strconv.FormatFloat(price, 'f', -1, 64),
	}, "|")
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:16])
}

func parseEmailFloat(s string) (float64, error) {
	s = strings.ReplaceAll(strings.TrimSpace(s), ",", "")
	return strconv.ParseFloat(s, 64)
}
