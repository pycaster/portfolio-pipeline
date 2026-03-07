// Package emailpoller fetches unseen emails from an IMAP mailbox and returns
// their bodies. It marks each fetched message as seen so subsequent runs skip it.
package emailpoller

import (
	"fmt"
	"io"
	"net/textproto"
	"strings"

	imapclient "github.com/emersion/go-imap/client"
	"github.com/emersion/go-imap"
	"github.com/emersion/go-message/mail"
)

// Config holds IMAP connection settings.
type Config struct {
	Addr     string // e.g. "imap.gmail.com:993"
	User     string
	Password string
	Folder   string // e.g. "INBOX"
	Sender   string // filter by From header, e.g. "noreply@robinhood.com"
}

// Message is a fetched email with its UID and decoded body.
type Message struct {
	UID  uint32
	Body string
}

// Poller connects to an IMAP server and fetches unseen messages.
type Poller struct {
	cfg Config
}

func New(cfg Config) *Poller { return &Poller{cfg: cfg} }

// FetchUnseen connects, searches for unseen messages from cfg.Sender,
// fetches their bodies, marks them as seen, and disconnects.
// Returns an empty slice if there are no new messages.
func (p *Poller) FetchUnseen() ([]Message, error) {
	c, err := imapclient.DialTLS(p.cfg.Addr, nil)
	if err != nil {
		return nil, fmt.Errorf("imap dial: %w", err)
	}
	defer c.Logout() //nolint:errcheck

	if err := c.Login(p.cfg.User, p.cfg.Password); err != nil {
		return nil, fmt.Errorf("imap login: %w", err)
	}

	if _, err := c.Select(p.cfg.Folder, false); err != nil {
		return nil, fmt.Errorf("imap select %q: %w", p.cfg.Folder, err)
	}

	criteria := imap.NewSearchCriteria()
	criteria.WithoutFlags = []string{imap.SeenFlag}
	criteria.Header = make(textproto.MIMEHeader)
	criteria.Header.Add("From", p.cfg.Sender)

	uids, err := c.UidSearch(criteria)
	if err != nil {
		return nil, fmt.Errorf("imap search: %w", err)
	}
	if len(uids) == 0 {
		return nil, nil
	}

	seqset := new(imap.SeqSet)
	seqset.AddNum(uids...)

	msgCh := make(chan *imap.Message, 10)
	fetchErr := make(chan error, 1)
	go func() {
		fetchErr <- c.UidFetch(seqset, []imap.FetchItem{imap.FetchRFC822}, msgCh)
	}()

	var msgs []Message
	for raw := range msgCh {
		r := raw.GetBody(&imap.BodySectionName{})
		if r == nil {
			continue
		}
		body, err := extractBody(r)
		if err != nil {
			return nil, fmt.Errorf("uid %d: parse body: %w", raw.Uid, err)
		}
		msgs = append(msgs, Message{UID: raw.Uid, Body: body})
	}
	if err := <-fetchErr; err != nil {
		return nil, fmt.Errorf("imap fetch: %w", err)
	}

	// Mark all fetched UIDs as seen.
	item := imap.FormatFlagsOp(imap.AddFlags, true)
	flags := []interface{}{imap.SeenFlag}
	if err := c.UidStore(seqset, item, flags, nil); err != nil {
		return msgs, fmt.Errorf("imap mark seen: %w", err)
	}

	return msgs, nil
}

// extractBody returns the decoded body of the email.
// Prefers text/plain; falls back to text/html (returned as-is for callers to parse).
// If MIME parsing fails, returns the raw body bytes.
func extractBody(r io.Reader) (string, error) {
	mr, err := mail.CreateReader(r)
	if err != nil {
		// Single-part or unparseable — return raw bytes.
		b, readErr := io.ReadAll(r)
		if readErr != nil {
			return "", fmt.Errorf("read raw body: %w", readErr)
		}
		return string(b), nil
	}

	var htmlBody string
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("next mime part: %w", err)
		}
		ct := part.Header.Get("Content-Type")
		b, err := io.ReadAll(part.Body)
		if err != nil {
			return "", fmt.Errorf("read part body: %w", err)
		}
		if strings.HasPrefix(ct, "text/plain") {
			return string(b), nil
		}
		if strings.HasPrefix(ct, "text/html") && htmlBody == "" {
			htmlBody = string(b)
		}
	}

	if htmlBody != "" {
		return htmlBody, nil
	}
	return "", fmt.Errorf("no text/plain or text/html part found in MIME message")
}
