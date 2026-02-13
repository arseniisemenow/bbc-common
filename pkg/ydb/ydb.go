package ydb

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	yc "github.com/ydb-platform/ydb-go-yc-metadata"
)

var (
	db   *ydb.Driver
	once sync.Once
)

// GetConnection returns a YDB connection, creating it if needed
func GetConnection(ctx context.Context) (*ydb.Driver, error) {
	var initErr error
	once.Do(func() {
		endpoint := os.Getenv("YDB_ENDPOINT")
		database := os.Getenv("YDB_DATABASE")

		log.Printf("[YDB] Initializing connection: endpoint=%s database=%s", endpoint, database)

		if endpoint == "" {
			initErr = fmt.Errorf("YDB_ENDPOINT environment variable not set")
			return
		}
		if database == "" {
			initErr = fmt.Errorf("YDB_DATABASE environment variable not set")
			return
		}

		connectionString := endpoint + "/?database=" + database
		log.Printf("[YDB] Connection string: %s", connectionString)

		db, initErr = ydb.Open(ctx, connectionString,
			yc.WithCredentials(), // Use instance metadata service for authentication
			yc.WithInternalCA(),  // Append Yandex Cloud certificates
		)

		if initErr != nil {
			log.Printf("[YDB] Failed to open connection: %v", initErr)
		} else {
			log.Printf("[YDB] Successfully opened connection")
		}
	})

	if db == nil && initErr == nil {
		log.Printf("[YDB] WARNING: db is nil but initErr is also nil")
	}

	return db, initErr
}

// Query executes a query and returns the result set
func Query(ctx context.Context, sql string, params ...table.ParameterOption) (result.Result, error) {
	driver, err := GetConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get YDB connection: %w", err)
	}

	log.Printf("[YDB] Querying SQL (first 100 chars): %s", truncateString(sql, 100))
	var res result.Result
	err = driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, r, err := s.Execute(ctx, table.DefaultTxControl(), sql, table.NewQueryParameters(params...))
		if err != nil {
			log.Printf("[YDB] Execute failed: %v", err)
			return err
		}
		if err := r.NextResultSetErr(ctx); err != nil {
			log.Printf("[YDB] NextResultSetErr failed: %v", err)
			r.Close()
			return err
		}
		res = r
		log.Printf("[YDB] Execute succeeded, got result set")
		return nil
	}, table.WithIdempotent())

	if err != nil {
		log.Printf("[YDB] Do failed: %v", err)
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	return res, nil
}

// Exec executes a query that doesn't return results
func Exec(ctx context.Context, sql string, params ...table.ParameterOption) error {
	driver, err := GetConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get YDB connection: %w", err)
	}

	log.Printf("[YDB] Executing SQL (first 100 chars): %s", truncateString(sql, 100))
	err = driver.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		res, err := tx.Execute(ctx, sql, table.NewQueryParameters(params...))
		if err != nil {
			log.Printf("[YDB] Execute failed: %v", err)
			return err
		}
		if err = res.Err(); err != nil {
			log.Printf("[YDB] Result error: %v", err)
			return err
		}
		if err = res.Close(); err != nil {
			log.Printf("[YDB] Close failed: %v", err)
			return err
		}
		log.Printf("[YDB] Execute succeeded, DoTx will commit on callback return")
		return nil
	}, table.WithIdempotent())

	if err != nil {
		log.Printf("[YDB] DoTx failed: %v", err)
	} else {
		log.Printf("[YDB] DoTx succeeded - transaction should be committed")
	}
	return err
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// DoTx executes a function within a transaction
func DoTx(ctx context.Context, fn func(ctx context.Context, tx table.TransactionActor) error) error {
	driver, err := GetConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get YDB connection: %w", err)
	}

	return driver.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		return fn(ctx, tx)
	}, table.WithIdempotent())
}

// NewParameter creates a new query parameter
func NewParameter(name string, value any) table.ParameterOption {
	return table.ValueParam(name, value.(types.Value))
}

// TablePathPrefix returns the PRAGMA TablePathPrefix directive
func TablePathPrefix(path string) string {
	if path == "" {
		database := os.Getenv("YDB_DATABASE")
		if database == "" {
			return ""
		}
		return fmt.Sprintf("PRAGMA TablePathPrefix(\"%s\");", database)
	}
	return fmt.Sprintf("PRAGMA TablePathPrefix(\"%s\");", path)
}
