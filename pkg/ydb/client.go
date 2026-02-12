package ydb

import (
	"context"

	"github.com/arseniisemenow/bbc-common/pkg/models"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// YDBClient implements Database interface
type YDBClient struct {
	driver *ydb.Driver
}

// NewClient creates a new YDB client
func NewClient(driver *ydb.Driver) *YDBClient {
	return &YDBClient{driver: driver}
}

// DoTx executes a transaction
func (c *YDBClient) DoTx(ctx context.Context, fn func(ctx context.Context, tx table.TransactionActor) error) error {
	return c.driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), "", nil)
		tx, err := s.BeginTransaction(ctx, table.SerializableReadWriteTxControl())
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		if err := fn(ctx, tx); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
}

// Query executes a query and returns result
func (c *YDBClient) Query(ctx context.Context, sql string, params *table.QueryParameters) (result.Result, error) {
	var res result.Result
	err := c.driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		var err error
		res, err = s.Execute(ctx, table.SerializableReadWriteTxControl(), sql, params, options.WithCollectStatsModeBasic())
		return err
	})
	return res, err
}

// Exec executes a query without returning result
func (c *YDBClient) Exec(ctx context.Context, sql string, params *table.QueryParameters) error {
	return c.driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), sql, params)
		return err
	})
}

// Params creates query parameters
func Params(p ...table.ParameterOption) *table.QueryParameters {
	return table.NewQueryParameters(p...)
}

// ParamValue creates a parameter value
func ParamValue(name string, v types.Value) table.ParameterOption {
	return table.ValueParam(name, v)
}
