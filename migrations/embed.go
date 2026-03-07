// Package migrations embeds all SQL migration files into the binary.
// Import this package and pass Files to store.Migrate().
package migrations

import "embed"

//go:embed *.sql
var Files embed.FS
