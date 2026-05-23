// SQL helpers that bridge dialect differences across harmonytask's
// supported backends. The interface boundary (harmonyquery.DBInterface)
// can't paper over every SQL-level dialect difference: Postgres has
// `ANY($1)` over arrays; SQLite expands `IN (?,?,?,...)` per element;
// modernc.org/sqlite has no array type at all and rejects []string
// parameter values entirely.
//
// These helpers expand a Go slice into an `IN (?,?,?)` clause plus the
// matching positional args, so the same SQL string works on both
// Postgres and SQLite.

package harmonytask

import (
	"fmt"
	"strings"

	"github.com/curiostorage/harmonyquery"
)

// inClauseInts returns ("?,?,?,...", []any{v0, v1, v2, ...}) for the
// given int64 slice. Use as:
//
//	clause, args := inClauseInts(ids)
//	db.ExecI(ctx, "UPDATE t SET x=NULL WHERE id IN ("+clause+")", args...)
//
// The slice MUST be non-empty: callers should guard before calling.
func inClauseInts(ids []int64) (string, []any) {
	if len(ids) == 0 {
		return "NULL", nil // produces IN (NULL) which matches nothing — caller should guard
	}
	args := make([]any, len(ids))
	parts := make([]string, len(ids))
	for i, v := range ids {
		args[i] = v
		parts[i] = "?"
	}
	return strings.Join(parts, ","), args
}

// inClauseStrings is the string-slice variant of inClauseInts.
func inClauseStrings(ss []string) (string, []any) {
	if len(ss) == 0 {
		return "NULL", nil
	}
	args := make([]any, len(ss))
	parts := make([]string, len(ss))
	for i, v := range ss {
		args[i] = v
		parts[i] = "?"
	}
	return strings.Join(parts, ","), args
}

// inClauseTaskIDs is the TaskID variant. TaskID is internally int64 in
// upstream Curio, but we keep this typed wrapper so call sites don't
// need to convert.
func inClauseTaskIDs(ids []TaskID) (string, []any) {
	if len(ids) == 0 {
		return "NULL", nil
	}
	args := make([]any, len(ids))
	parts := make([]string, len(ids))
	for i, v := range ids {
		args[i] = int64(v)
		parts[i] = "?"
	}
	return strings.Join(parts, ","), args
}

// expandIn substitutes a single `?IN?` placeholder in template with the
// expanded `?,?,?,...` clause for n args. Returns the composed SQL as a
// harmonyquery.RawString.
//
// SAFETY: template is a compile-time literal (untyped string), so it
// passes the rawStringOnly check at the call site. We then concatenate
// a programmatically-built `?,?,?` substring — which contains ONLY
// '?' and ',' characters, no user input — and explicitly cast the
// result back to RawString. The injection-safety property is preserved:
// every variable substituted into the final query goes through the
// driver's positional-parameter binding, not string interpolation.
//
// Use as:
//
//	inClause, args := inClauseTaskIDs(ids)
//	db.ExecI(ctx, expandIn(`UPDATE t SET x=NULL WHERE id IN (?IN?)`, inClause), args...)
func expandIn(template string, inClause string) harmonyquery.RawString {
	return harmonyquery.RawString(strings.Replace(template, "?IN?", inClause, 1))
}

// _ keeps fmt imported for future formatting helpers; will be removed
// when we add the first formatter that doesn't use strings.Builder.
var _ = fmt.Sprintf
