package wal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"gotest.tools/v3/assert"
)

// =============================================================================
// TEST HELPERS
// =============================================================================

// createTestWAL creates a temporary WAL file for testing.
// Returns the WAL instance and the temp directory path.
// The caller should defer cleanup using cleanupTestWAL.
func createTestWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "test-wal")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	walPath := filepath.Join(tempDir, "test-wal")
	dbName := "test-db"
	wal, err := NewWAL(walPath, dbName)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	return wal, tempDir
	// TODO: Create temp directory
	// TODO: Create WAL in temp directory
	// TODO: Return WAL and temp dir path
	return nil, ""
}

// cleanupTestWAL removes the temporary WAL directory and files.
func cleanupTestWAL(t *testing.T, tempDir string) {
	t.Helper()
	// TODO: Remove temp directory
	_ = os.RemoveAll(tempDir)
}

// createTestJSON creates a json.RawMessage from a map for testing.
func createTestJSON(t *testing.T, data map[string]interface{}) json.RawMessage {
	t.Helper()

	// TODO: Marshal map to JSON
	bytes, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("failed to create test JSON: %v", err)
	}
	return json.RawMessage(bytes)
}

// =============================================================================
// SUITE 1: WAL WRITER TESTS
// =============================================================================

// TestBeginTransaction verifies that BeginTransaction:
// - Writes a BeginTxn record to WAL
// - Returns sequential LSNs for each new transaction
// - Creates proper transaction state tracking
func TestBeginTransaction(t *testing.T) {
	wal, tempDir := createTestWAL(t)
	defer cleanupTestWAL(t, tempDir)

	var txOne uint64 = 1
	var txTwo uint64 = 2

	txIDOne, err := wal.BeginTransaction(txOne)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	assert.Equal(t, txOne, txIDOne)

	txIDTwo, err := wal.BeginTransaction(txTwo)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	assert.Equal(t, txTwo, txIDTwo)
	assert.Equal(t, wal.verifyActiveTxn(txIDOne), nil)
	assert.Equal(t, wal.verifyActiveTxn(txIDTwo), nil)
	// TODO: Create test WAL
	// TODO: Begin transaction 1
	// TODO: Verify LSN returned is 1
	// TODO: Begin transaction 2
	// TODO: Verify LSN returned is 2
	// TODO: Verify both transactions are tracked as active

}

// TestLogInsert verifies that LogInsert:
// - Writes an Insert record with table name, key, and value
// - Encodes the JSON payload correctly
// - Associates the record with the correct transaction
func TestLogInsert(t *testing.T) {
	wal, tempDir := createTestWAL(t)
	defer cleanupTestWAL(t, tempDir)

	var txOne uint64 = 1

	txIDOne, err := wal.BeginTransaction(txOne)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	id, err := wal.LogInsert(txIDOne, "users", "1", createTestJSON(t, map[string]interface{}{"name": "Alice"}))
	assert.Equal(t, nil, err)
	assert.Equal(t, uint64(2), id)
	assert.Equal(t, wal.verifyActiveTxn(txIDOne), nil)

	// TODO: Create test WAL
	// TODO: Begin a transaction
	// TODO: Log an insert with table="users", key="1", value={"name":"Alice"}
	// TODO: Verify record is written (by reading back)
	// TODO: Verify table name, key, and value match

}

// TestLogUpdate verifies that LogUpdate:
// - Writes an Update record with old and new values
// - Both old and new values are properly serialized
// - Key matches the primary key of the row
func TestLogUpdate(t *testing.T) {
	wal, tempDir := createTestWAL(t)
	defer cleanupTestWAL(t, tempDir)

	var txOne uint64 = 1

	id, err := wal.LogUpdate(txOne, "users", "1", createTestJSON(t, map[string]interface{}{"name": "Alice"}), createTestJSON(t, map[string]interface{}{"name": "Bob"}))
	assert.Equal(t, nil, err)
	assert.Equal(t, uint64(2), id)
	assert.Equal(t, wal.verifyActiveTxn(txOne), nil)

	// TODO: Create test WAL
	// TODO: Begin a transaction
	// TODO: Log an update: table="users", key="1", old={"name":"Alice"}, new={"name":"Bob"}
	// TODO: Verify record contains both old and new values
}

// TestLogDelete verifies that LogDelete:
// - Writes a Delete record with the old value (for potential undo)
// - Record is associated with the correct transaction
func TestLogDelete(t *testing.T) {
	// TODO: Create test WAL
	// TODO: Begin a transaction
	// TODO: Log a delete: table="users", key="1", old={"name":"Alice"}
	// TODO: Verify record is written with old value preserved
	t.Skip("Not implemented yet")
}

// TestCommit verifies that Commit:
// - Writes a Commit record to WAL
// - Calls fsync to ensure durability
// - Marks the transaction as no longer active
func TestCommit(t *testing.T) {
	// TODO: Create test WAL
	// TODO: Begin a transaction
	// TODO: Log some operations
	// TODO: Commit
	// TODO: Verify commit record is written
	// TODO: Verify transaction is no longer in active map
	t.Skip("Not implemented yet")
}

// TestAbort verifies that Abort:
// - Writes an Abort record to WAL
// - Marks the transaction as aborted (not to be replayed)
func TestAbort(t *testing.T) {
	// TODO: Create test WAL
	// TODO: Begin a transaction
	// TODO: Log some operations
	// TODO: Abort
	// TODO: Verify abort record is written
	t.Skip("Not implemented yet")
}

// TestWriteCheckpoint verifies that WriteCheckpoint:
// - Writes a Checkpoint record with table checksums
// - Database CRC is included
// - Checkpoint can be used as recovery point
func TestWriteCheckpoint(t *testing.T) {
	// TODO: Create test WAL
	// TODO: Begin and commit some transactions
	// TODO: Write checkpoint with table checksums
	// TODO: Verify checkpoint record is written
	// TODO: Verify checksums are in the record
	t.Skip("Not implemented yet")
}

// TestConcurrentTransactions verifies that multiple transactions:
// - Can be active simultaneously
// - Each gets unique TxIDs
// - Commits/Aborts are independent
func TestConcurrentTransactions(t *testing.T) {
	// TODO: Create test WAL
	// TODO: Begin tx1, tx2, tx3
	// TODO: Log ops to each in interleaved order
	// TODO: Commit tx1, Abort tx2, Commit tx3
	// TODO: Verify all records have correct TxIDs
	t.Skip("Not implemented yet")
}

// TestLSNMonotonicity verifies that LSNs:
// - Are always increasing
// - Never duplicate
// - Increment correctly across all record types
func TestLSNMonotonicity(t *testing.T) {
	// TODO: Create test WAL
	// TODO: Write various record types
	// TODO: Read all records back
	// TODO: Verify LSNs are strictly increasing
	t.Skip("Not implemented yet")
}

// =============================================================================
// FILE VERIFICATION HELPERS
// =============================================================================

// verifyWALFileExists checks that a WAL file was created at the expected path.
func verifyWALFileExists(t *testing.T, tempDir, dbName string) {
	t.Helper()
	walPath := filepath.Join(tempDir, dbName+".wal")
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Fatalf("WAL file not found at %s", walPath)
	}
}

// verifyWALNotEmpty checks that the WAL file has content.
func verifyWALNotEmpty(t *testing.T, tempDir, dbName string) {
	t.Helper()
	walPath := filepath.Join(tempDir, dbName+".wal")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("failed to stat WAL file: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("WAL file is empty")
	}
}
