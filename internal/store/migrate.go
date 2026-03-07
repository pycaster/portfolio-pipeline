package store

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"
)

// Migration represents a single versioned schema change.
type Migration struct {
	Version uint32
	Name    string // stem, e.g. "000001_transactions"
	SQL     string
}

// Migrate applies all pending up migrations from the provided FS.
// Safe to call on every startup — already-applied versions are skipped.
// The _migrations tracker table is bootstrapped automatically.
func (s *Store) Migrate(ctx context.Context, files fs.FS) error {
	if err := s.bootstrap(ctx); err != nil {
		return fmt.Errorf("migrate: bootstrap: %w", err)
	}

	applied, err := s.appliedVersions(ctx)
	if err != nil {
		return fmt.Errorf("migrate: querying applied versions: %w", err)
	}

	migrations, err := loadUpMigrations(files)
	if err != nil {
		return fmt.Errorf("migrate: loading files: %w", err)
	}

	pending := 0
	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}
		pending++
		slog.Info("applying migration", "version", m.Version, "name", m.Name)

		if err := s.execStatements(ctx, m.SQL); err != nil {
			return fmt.Errorf("migrate: %06d (%s): %w", m.Version, m.Name, err)
		}
		if err := s.conn.Exec(ctx,
			`INSERT INTO portfolio._migrations (version, name) VALUES (?, ?)`,
			m.Version, m.Name,
		); err != nil {
			return fmt.Errorf("migrate: recording %06d: %w", m.Version, err)
		}
		slog.Info("migration applied", "version", m.Version)
	}

	if pending == 0 {
		slog.Info("schema up to date")
	}
	return nil
}

// MigrateStatus prints applied vs pending migrations to stdout.
func (s *Store) MigrateStatus(ctx context.Context, files fs.FS) error {
	if err := s.bootstrap(ctx); err != nil {
		return fmt.Errorf("migrate status: %w", err)
	}
	applied, err := s.appliedVersions(ctx)
	if err != nil {
		return fmt.Errorf("migrate status: %w", err)
	}
	migrations, err := loadUpMigrations(files)
	if err != nil {
		return fmt.Errorf("migrate status: %w", err)
	}

	fmt.Printf("  %-8s  %-6s  %s\n", "status", "ver", "name")
	fmt.Println("  " + strings.Repeat("-", 48))
	for _, m := range migrations {
		status := "pending"
		if applied[m.Version] {
			status = "applied"
		}
		fmt.Printf("  %-8s  %06d  %s\n", status, m.Version, m.Name)
	}
	return nil
}

// bootstrap creates the database and the _migrations tracker if absent.
func (s *Store) bootstrap(ctx context.Context) error {
	for _, stmt := range []string{
		`CREATE DATABASE IF NOT EXISTS portfolio`,
		`CREATE TABLE IF NOT EXISTS portfolio._migrations (
			version    UInt32,
			name       String,
			applied_at DateTime DEFAULT now()
		) ENGINE = ReplacingMergeTree(applied_at)
		ORDER BY version`,
	} {
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) appliedVersions(ctx context.Context) (map[uint32]bool, error) {
	rows, err := s.conn.Query(ctx,
		`SELECT version FROM portfolio._migrations FINAL ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[uint32]bool)
	for rows.Next() {
		var v uint32
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

// loadUpMigrations reads *.up.sql files from files, parses version numbers
// from filenames (NNNNNN_name.up.sql), and returns them sorted ascending.
func loadUpMigrations(files fs.FS) ([]Migration, error) {
	var migrations []Migration

	err := fs.WalkDir(files, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		base := path.Base(p)
		if !strings.HasSuffix(base, ".up.sql") {
			return nil
		}

		stem := strings.TrimSuffix(base, ".up.sql")
		parts := strings.SplitN(stem, "_", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid filename %q: want NNNNNN_description.up.sql", base)
		}
		v, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			return fmt.Errorf("non-numeric version in %q: %w", base, err)
		}

		content, err := fs.ReadFile(files, p)
		if err != nil {
			return err
		}
		migrations = append(migrations, Migration{
			Version: uint32(v),
			Name:    stem,
			SQL:     string(content),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})
	return migrations, nil
}

// execStatements splits a SQL file into individual statements and executes
// each one (ClickHouse does not support multi-statement queries).
//
// Splitting is comment-aware: semicolons inside -- line comments are ignored,
// so comments like "-- BTO opens; STC closes" do not produce spurious splits.
func (s *Store) execStatements(ctx context.Context, sql string) error {
	for _, stmt := range splitSQL(sql) {
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("%q: %w", truncate(stmt, 80), err)
		}
	}
	return nil
}

// splitSQL parses a SQL string into individual statements, splitting on
// semicolons while ignoring any semicolons inside -- line comments.
func splitSQL(sql string) []string {
	var stmts []string
	var cur strings.Builder
	inComment := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if inComment {
			if ch == '\n' {
				inComment = false
				cur.WriteByte(ch)
			}
			// drop comment characters — they are not part of the statement
			continue
		}

		// Detect -- comment start
		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			inComment = true
			i++ // skip second '-'
			continue
		}

		if ch == ';' {
			if stmt := strings.TrimSpace(cur.String()); stmt != "" {
				stmts = append(stmts, stmt)
			}
			cur.Reset()
			continue
		}

		cur.WriteByte(ch)
	}

	if stmt := strings.TrimSpace(cur.String()); stmt != "" {
		stmts = append(stmts, stmt)
	}
	return stmts
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
