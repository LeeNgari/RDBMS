package manager

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/leengari/mini-rdbms/internal/domain/schema"
	"github.com/leengari/mini-rdbms/internal/domain/transaction"
	"github.com/leengari/mini-rdbms/internal/query/indexing"
	"github.com/leengari/mini-rdbms/internal/storage/engine"
)

// Registry manages loaded databases in a thread-safe way
type Registry struct {
	mu            sync.RWMutex
	loaded        map[string]*schema.Database
	walManagers   map[string]*WALManager // Per-database WAL managers
	basePath      string
	storageEngine engine.StorageEngine
	walEnabled    bool // Whether WAL is enabled globally
}

// NewRegistry creates a new database registry with the given storage engine
func NewRegistry(basePath string, storageEngine engine.StorageEngine) *Registry {
	return NewRegistryWithWAL(basePath, storageEngine, true) // WAL enabled by default
}

// NewRegistryWithWAL creates a new database registry with explicit WAL configuration
func NewRegistryWithWAL(basePath string, storageEngine engine.StorageEngine, walEnabled bool) *Registry {
	return &Registry{
		loaded:        make(map[string]*schema.Database),
		walManagers:   make(map[string]*WALManager),
		basePath:      basePath,
		storageEngine: storageEngine,
		walEnabled:    walEnabled,
	}
}

// Get loads a database (or returns cached one) and ensures indexes are built
// Deprecated: Use GetWithWAL for WAL support
func (r *Registry) Get(name string) (*schema.Database, error) {
	db, _, err := r.GetWithWAL(name)
	return db, err
}

// GetWithWAL loads a database with its WAL manager
// If WAL recovery is needed, it will be performed before returning
func (r *Registry) GetWithWAL(name string) (*schema.Database, *WALManager, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check cache
	if db, ok := r.loaded[name]; ok {
		walMgr := r.walManagers[name]
		return db, walMgr, nil
	}

	// Load from disk using storage engine
	dbPath := filepath.Join(r.basePath, name)
	db, err := r.storageEngine.LoadDatabase(dbPath)
	if err != nil {
		return nil, nil, err
	}

	// Build Indexes
	if err := indexing.BuildDatabaseIndexes(db); err != nil {
		return nil, nil, fmt.Errorf("failed to build indexes: %w", err)
	}

	// Initialize WAL manager if enabled
	var walMgr *WALManager
	if r.walEnabled {
		walMgr, err = NewWALManager(dbPath, name, true)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create WAL manager: %w", err)
		}

		// Check if WAL file exists and needs recovery
		walPath := filepath.Join(dbPath, name+".wal")
		if _, statErr := os.Stat(walPath); statErr == nil {
			// WAL file exists - perform recovery
			result, recoverErr := walMgr.Recover()
			if recoverErr != nil {
				walMgr.Close()
				return nil, nil, fmt.Errorf("WAL recovery failed (refusing to start): %w", recoverErr)
			}

			// Replay operations if any
			if result != nil && (len(result.InsertOps) > 0 || len(result.UpdateOps) > 0 || len(result.DeleteOps) > 0) {
				slog.Info("WAL: Replaying operations",
					"database", name,
					"inserts", len(result.InsertOps),
					"updates", len(result.UpdateOps),
					"deletes", len(result.DeleteOps),
				)

				target := NewDatabaseReplayTarget(db)
				if replayErr := result.ReplayAll(target); replayErr != nil {
					walMgr.Close()
					return nil, nil, fmt.Errorf("WAL replay failed (refusing to start): %w", replayErr)
				}

				// Rebuild indexes after replay
				if indexErr := indexing.BuildDatabaseIndexes(db); indexErr != nil {
					walMgr.Close()
					return nil, nil, fmt.Errorf("failed to rebuild indexes after WAL replay: %w", indexErr)
				}

				slog.Info("WAL: Recovery complete", "database", name)
			}
		}

		r.walManagers[name] = walMgr
	}

	r.loaded[name] = db
	return db, walMgr, nil
}

// Create creates a new database
func (r *Registry) Create(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.loaded[name]; ok {
		return fmt.Errorf("database '%s' already exists (loaded)", name)
	}

	return r.storageEngine.CreateDatabase(name, r.basePath)
}

// Drop unloads and deletes a database
func (r *Registry) Drop(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Close WAL manager if exists
	if walMgr, ok := r.walManagers[name]; ok {
		walMgr.Close()
		delete(r.walManagers, name)
	}

	delete(r.loaded, name)
	return r.storageEngine.DropDatabase(name, r.basePath)
}

// Rename saves, unloads, and renames a database
func (r *Registry) Rename(oldName, newName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Close old WAL manager if exists
	if walMgr, ok := r.walManagers[oldName]; ok {
		walMgr.Close()
		delete(r.walManagers, oldName)
	}

	// If loaded, we must unload/save
	if db, ok := r.loaded[oldName]; ok {
		// Create a transaction for the save operation
		tx := transaction.NewTransaction()
		defer tx.Close()

		if err := r.storageEngine.SaveDatabase(db, tx); err != nil {
			return fmt.Errorf("failed to save database before rename: %w", err)
		}
		delete(r.loaded, oldName)
	}

	return r.storageEngine.RenameDatabase(oldName, newName, r.basePath)
}

// SaveAll saves all currently loaded databases and writes checkpoints
func (r *Registry) SaveAll(tx *transaction.Transaction) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for name, db := range r.loaded {
		if err := r.storageEngine.SaveDatabase(db, tx); err != nil {
			slog.Error("failed to save database", "name", db.Name, "error", err)
			continue
		}

		// Write checkpoint after successful save
		if walMgr, ok := r.walManagers[name]; ok {
			if err := walMgr.WriteCheckpoint(db); err != nil {
				slog.Error("failed to write checkpoint", "name", name, "error", err)
			}
		}
	}
}

// CloseAll closes all WAL managers (call on shutdown)
func (r *Registry) CloseAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for name, walMgr := range r.walManagers {
		if err := walMgr.Close(); err != nil {
			slog.Error("failed to close WAL manager", "name", name, "error", err)
		}
	}
	r.walManagers = make(map[string]*WALManager)
}

// List returns a list of all available databases
func (r *Registry) List() ([]string, error) {
	return r.storageEngine.ListDatabases(r.basePath)
}

// IsWALEnabled returns whether WAL is enabled for this registry
func (r *Registry) IsWALEnabled() bool {
	return r.walEnabled
}
