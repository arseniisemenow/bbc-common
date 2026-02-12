package ydb

import (
	"context"
	"os"
	"sync"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	yc "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

var (
	db       *ydb.Driver
	once     sync.Once
	initErr  error
)

// GetConnection returns a singleton YDB connection
func GetConnection(ctx context.Context) (*ydb.Driver, error) {
	once.Do(func() {
		endpoint := os.Getenv("YDB_ENDPOINT")
		database := os.Getenv("YDB_DATABASE")

		if endpoint == "" || database == "" {
			initErr = ErrMissingConfig
			return
		}

		db, initErr = ydb.Open(
			ctx,
			endpoint+database,
			yc.WithCredentials(yc.NewInstanceMetadataCredentials(yc.WithInternalCA())),
		)
	})

	return db, initErr
}
