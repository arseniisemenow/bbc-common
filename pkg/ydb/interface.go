package ydb

import (
	"context"

	"github.com/arseniisemenow/bbc-common/pkg/models"
)

// Database defines the interface for database operations
type Database interface {
	// User operations
	GetUserByTelegramChatID(ctx context.Context, chatID int64) (*models.User, error)
	UpsertUser(ctx context.Context, user *models.User) error
	UpdateUserStatus(ctx context.Context, chatID int64, status models.UserStatus) error
	GetActiveUsers(ctx context.Context) ([]models.User, error)

	// Token operations
	GetUserTokens(ctx context.Context, chatID int64) (*models.UserTokens, error)
	StoreUserTokens(ctx context.Context, tokens *models.UserTokens) error
	DeleteUserTokens(ctx context.Context, chatID int64) error

	// Subscription operations
	CreateSearchSubscription(ctx context.Context, sub *models.SearchSubscription) error
	GetSearchSubscriptionsByUser(ctx context.Context, chatID int64) ([]models.SearchSubscription, error)
	GetActiveSubscriptions(ctx context.Context) ([]models.SearchSubscription, error)
	UpdateSubscriptionLastChecked(ctx context.Context, subID string) error
	DeleteSearchSubscription(ctx context.Context, subID string) error
	SetSubscriptionActive(ctx context.Context, subID string, active bool) error

	// Notification operations
	CreateNotification(ctx context.Context, notif *models.Notification) error
	GetNotificationByTrip(ctx context.Context, chatID int64, subID, tripID string) (*models.Notification, error)
	UpdateNotificationMessageID(ctx context.Context, notifID string, messageID int) error
}
