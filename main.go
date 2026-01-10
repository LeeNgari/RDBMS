package main

import (
    "os" 

    "github.com/leengari/mini-rdbms/internal/logging"
    "github.com/leengari/mini-rdbms/internal/storage"
	"github.com/leengari/mini-rdbms/internal/engine"
)

func main() {
    log, closeFn := logging.SetupLogger()
    
    defer closeFn()

    log.Info("Starting application...")
    
    
    db, err := storage.LoadDatabase("databases/testdb", log)
    if err != nil {
        log.Error("failed to load database", "error", err)
        closeFn() 
        os.Exit(1)
    }
	if err := engine.BuildDatabaseIndexes(db); err != nil {
	log.Error("Index build failed", "error", err)
        closeFn() 
        os.Exit(1)
	}
    _ = db

    log.Info("Application ready")
}