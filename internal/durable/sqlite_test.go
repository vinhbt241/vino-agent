package durable

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// sqliteUserVersion reads the schema version stamped into the database file,
// through a fresh connection so the assertion observes the on-disk contract
// rather than the store's in-memory state.
func sqliteUserVersion(t *testing.T, path string) int {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var v int
	if err := db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestSQLiteMigrationsApply(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.db")

	store, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	if v := sqliteUserVersion(t, path); v != 1 {
		t.Fatalf("user_version = %d, want 1 after fresh open", v)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Reopen over the same file: idempotent, no duplicate-migration error.
	store2, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close(context.Background())
	if v := sqliteUserVersion(t, path); v != 1 {
		t.Fatalf("user_version = %d after reopen, want 1", v)
	}
}

func TestSQLiteSeqIncreases(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "agent.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	convID, err := s.MintID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seq1, err := s.Commit(ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entryID, err := s.MintID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seq2, err := s.Commit(ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: entryID, ConversationID: convID, Kind: "user"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if seq2 <= seq1 {
		t.Fatalf("commit seq = %d then %d, want strictly increasing", seq1, seq2)
	}
}
