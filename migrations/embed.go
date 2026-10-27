package migrations

import "embed"

// Files contains the SQL migrations applied during service startup.
//
//go:embed *.sql
var Files embed.FS
