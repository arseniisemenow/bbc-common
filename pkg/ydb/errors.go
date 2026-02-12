package ydb

import "errors"

var (
	ErrMissingConfig    = errors.New("YDB_ENDPOINT and YDB_DATABASE must be set")
	ErrUserNotFound     = errors.New("user not found")
	ErrTokensNotFound   = errors.New("tokens not found")
	ErrSubscriptionNotFound = errors.New("subscription not found")
)
