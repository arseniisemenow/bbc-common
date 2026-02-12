package ydb

import (
	"context"
	"time"

	"github.com/arseniisemenow/bbc-common/pkg/models"
	"github.com/google/uuid"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// GetUserByTelegramChatID retrieves a user by their Telegram chat ID
func GetUserByTelegramChatID(ctx context.Context, chatID int64) (*models.User, error) {
	db, err := GetConnection(ctx)
	if err != nil {
		return nil, err
	}

	query := `
		DECLARE $chat_id AS Int64;
		SELECT telegram_chat_id, status, created_at, last_auth_success_at, last_auth_failure_at
		FROM users
		WHERE telegram_chat_id = $chat_id;
	`

	res, err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) (result.Result, error) {
		return s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$chat_id", types.Int64Value(chatID)),
			))
	})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.Next() {
		return nil, ErrUserNotFound
	}

	var user models.User
	var lastAuthSuccess, lastAuthFailure *time.Time
	if err := res.Scan(&user.TelegramChatID, &user.Status, &user.CreatedAt, &lastAuthSuccess, &lastAuthFailure); err != nil {
		return nil, err
	}
	user.LastAuthSuccessAt = lastAuthSuccess
	user.LastAuthFailureAt = lastAuthFailure

	return &user, nil
}

// UpsertUser creates or updates a user
func UpsertUser(ctx context.Context, user *models.User) error {
	db, err := GetConnection(ctx)
	if err != nil {
		return err
	}

	now := time.Now()
	if user.CreatedAt.IsZero() {
		user.CreatedAt = now
	}

	query := `
		DECLARE $telegram_chat_id AS Int64;
		DECLARE $status AS Utf8;
		DECLARE $created_at AS Timestamp;
		DECLARE $last_auth_success_at AS Optional<Timestamp>;
		DECLARE $last_auth_failure_at AS Optional<Timestamp>;

		UPSERT INTO users (telegram_chat_id, status, created_at, last_auth_success_at, last_auth_failure_at)
		VALUES ($telegram_chat_id, $status, $created_at, $last_auth_success_at, $last_auth_failure_at);
	`

	return db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		var lastSuccess, lastFailure *types.Value
		if user.LastAuthSuccessAt != nil {
			t := types.TimestampValueFromTime(*user.LastAuthSuccessAt)
			lastSuccess = &t
		}
		if user.LastAuthFailureAt != nil {
			t := types.TimestampValueFromTime(*user.LastAuthFailureAt)
			lastFailure = &t
		}

		_, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$telegram_chat_id", types.Int64Value(user.TelegramChatID)),
				table.ValueParam("$status", types.UTF8Value(string(user.Status))),
				table.ValueParam("$created_at", types.TimestampValueFromTime(user.CreatedAt)),
				table.ValueParam("$last_auth_success_at", types.OptionalValue(types.TypeTimestamp, lastSuccess)),
				table.ValueParam("$last_auth_failure_at", types.OptionalValue(types.TypeTimestamp, lastFailure)),
			))
		return err
	})
}

// UpdateUserStatus updates a user's status
func UpdateUserStatus(ctx context.Context, chatID int64, status models.UserStatus) error {
	db, err := GetConnection(ctx)
	if err != nil {
		return err
	}

	query := `
		DECLARE $chat_id AS Int64;
		DECLARE $status AS Utf8;
		UPDATE users SET status = $status WHERE telegram_chat_id = $chat_id;
	`

	return db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$chat_id", types.Int64Value(chatID)),
				table.ValueParam("$status", types.UTF8Value(string(status))),
			))
		return err
	})
}

// GetActiveUsers retrieves all active users
func GetActiveUsers(ctx context.Context) ([]models.User, error) {
	db, err := GetConnection(ctx)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT telegram_chat_id, status, created_at, last_auth_success_at, last_auth_failure_at
		FROM users
		WHERE status = 'active';
	`

	res, err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) (result.Result, error) {
		return s.Execute(ctx, table.SerializableReadWriteTxControl(), query, nil)
	})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var users []models.User
	for res.NextResultSet(ctx) {
		for res.Next() {
			var user models.User
			var lastAuthSuccess, lastAuthFailure *time.Time
			if err := res.Scan(&user.TelegramChatID, &user.Status, &user.CreatedAt, &lastAuthSuccess, &lastAuthFailure); err != nil {
				return nil, err
			}
			user.LastAuthSuccessAt = lastAuthSuccess
			user.LastAuthFailureAt = lastAuthFailure
			users = append(users, user)
		}
	}

	return users, nil
}

// GetUserTokens retrieves tokens for a user
func GetUserTokens(ctx context.Context, chatID int64) (*models.UserTokens, error) {
	db, err := GetConnection(ctx)
	if err != nil {
		return nil, err
	}

	query := `
		DECLARE $chat_id AS Int64;
		SELECT telegram_chat_id, access_token, refresh_token, user_id, datadome, app_token, created_at, updated_at
		FROM user_tokens
		WHERE telegram_chat_id = $chat_id;
	`

	res, err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) (result.Result, error) {
		return s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$chat_id", types.Int64Value(chatID)),
			))
	})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.Next() {
		return nil, ErrTokensNotFound
	}

	var tokens models.UserTokens
	if err := res.Scan(&tokens.TelegramChatID, &tokens.AccessToken, &tokens.RefreshToken,
		&tokens.UserID, &tokens.Datadome, &tokens.AppToken, &tokens.CreatedAt, &tokens.UpdatedAt); err != nil {
		return nil, err
	}

	return &tokens, nil
}

// StoreUserTokens stores or updates user tokens
func StoreUserTokens(ctx context.Context, tokens *models.UserTokens) error {
	db, err := GetConnection(ctx)
	if err != nil {
		return err
	}

	now := time.Now()
	if tokens.CreatedAt.IsZero() {
		tokens.CreatedAt = now
	}
	tokens.UpdatedAt = now

	query := `
		DECLARE $telegram_chat_id AS Int64;
		DECLARE $access_token AS Utf8;
		DECLARE $refresh_token AS Utf8;
		DECLARE $user_id AS Utf8;
		DECLARE $datadome AS Optional<Utf8>;
		DECLARE $app_token AS Optional<Utf8>;
		DECLARE $created_at AS Timestamp;
		DECLARE $updated_at AS Timestamp;

		UPSERT INTO user_tokens (telegram_chat_id, access_token, refresh_token, user_id, datadome, app_token, created_at, updated_at)
		VALUES ($telegram_chat_id, $access_token, $refresh_token, $user_id, $datadome, $app_token, $created_at, $updated_at);
	`

	return db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		var datadome, appToken *types.Value
		if tokens.Datadome != "" {
			v := types.UTF8Value(tokens.Datadome)
			datadome = &v
		}
		if tokens.AppToken != "" {
			v := types.UTF8Value(tokens.AppToken)
			appToken = &v
		}

		_, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$telegram_chat_id", types.Int64Value(tokens.TelegramChatID)),
				table.ValueParam("$access_token", types.UTF8Value(tokens.AccessToken)),
				table.ValueParam("$refresh_token", types.UTF8Value(tokens.RefreshToken)),
				table.ValueParam("$user_id", types.UTF8Value(tokens.UserID)),
				table.ValueParam("$datadome", types.OptionalValue(types.TypeUTF8, datadome)),
				table.ValueParam("$app_token", types.OptionalValue(types.TypeUTF8, appToken)),
				table.ValueParam("$created_at", types.TimestampValueFromTime(tokens.CreatedAt)),
				table.ValueParam("$updated_at", types.TimestampValueFromTime(tokens.UpdatedAt)),
			))
		return err
	})
}

// DeleteUserTokens deletes user tokens
func DeleteUserTokens(ctx context.Context, chatID int64) error {
	db, err := GetConnection(ctx)
	if err != nil {
		return err
	}

	query := `
		DECLARE $chat_id AS Int64;
		DELETE FROM user_tokens WHERE telegram_chat_id = $chat_id;
	`

	return db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$chat_id", types.Int64Value(chatID)),
			))
		return err
	})
}

// CreateSearchSubscription creates a new search subscription
func CreateSearchSubscription(ctx context.Context, sub *models.SearchSubscription) error {
	db, err := GetConnection(ctx)
	if err != nil {
		return err
	}

	if sub.ID == "" {
		sub.ID = uuid.New().String()
	}
	sub.CreatedAt = time.Now()
	sub.IsActive = true

	query := `
		DECLARE $id AS Utf8;
		DECLARE $telegram_chat_id AS Int64;
		DECLARE $from_place_id AS Utf8;
		DECLARE $from_place_name AS Utf8;
		DECLARE $to_place_id AS Utf8;
		DECLARE $to_place_name AS Utf8;
		DECLARE $departure_date AS Utf8;
		DECLARE $requested_seats AS Int32;
		DECLARE $is_active AS Bool;
		DECLARE $created_at AS Timestamp;

		INSERT INTO search_subscriptions (id, telegram_chat_id, from_place_id, from_place_name, to_place_id, to_place_name, departure_date, requested_seats, is_active, created_at)
		VALUES ($id, $telegram_chat_id, $from_place_id, $from_place_name, $to_place_id, $to_place_name, $departure_date, $requested_seats, $is_active, $created_at);
	`

	return db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$id", types.UTF8Value(sub.ID)),
				table.ValueParam("$telegram_chat_id", types.Int64Value(sub.TelegramChatID)),
				table.ValueParam("$from_place_id", types.UTF8Value(sub.FromPlaceID)),
				table.ValueParam("$from_place_name", types.UTF8Value(sub.FromPlaceName)),
				table.ValueParam("$to_place_id", types.UTF8Value(sub.ToPlaceID)),
				table.ValueParam("$to_place_name", types.UTF8Value(sub.ToPlaceName)),
				table.ValueParam("$departure_date", types.UTF8Value(sub.DepartureDate)),
				table.ValueParam("$requested_seats", types.Int32Value(int32(sub.RequestedSeats))),
				table.ValueParam("$is_active", types.BoolValue(sub.IsActive)),
				table.ValueParam("$created_at", types.TimestampValueFromTime(sub.CreatedAt)),
			))
		return err
	})
}

// GetSearchSubscriptionsByUser retrieves all subscriptions for a user
func GetSearchSubscriptionsByUser(ctx context.Context, chatID int64) ([]models.SearchSubscription, error) {
	db, err := GetConnection(ctx)
	if err != nil {
		return nil, err
	}

	query := `
		DECLARE $chat_id AS Int64;
		SELECT id, telegram_chat_id, from_place_id, from_place_name, to_place_id, to_place_name, departure_date, requested_seats, is_active, created_at, last_checked_at
		FROM search_subscriptions
		WHERE telegram_chat_id = $chat_id;
	`

	res, err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) (result.Result, error) {
		return s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$chat_id", types.Int64Value(chatID)),
			))
	})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var subs []models.SearchSubscription
	for res.NextResultSet(ctx) {
		for res.Next() {
			var sub models.SearchSubscription
			var lastChecked *time.Time
			if err := res.Scan(&sub.ID, &sub.TelegramChatID, &sub.FromPlaceID, &sub.FromPlaceName,
				&sub.ToPlaceID, &sub.ToPlaceName, &sub.DepartureDate, &sub.RequestedSeats,
				&sub.IsActive, &sub.CreatedAt, &lastChecked); err != nil {
				return nil, err
			}
			sub.LastCheckedAt = lastChecked
			subs = append(subs, sub)
		}
	}

	return subs, nil
}

// GetActiveSubscriptions retrieves all active subscriptions
func GetActiveSubscriptions(ctx context.Context) ([]models.SearchSubscription, error) {
	db, err := GetConnection(ctx)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT id, telegram_chat_id, from_place_id, from_place_name, to_place_id, to_place_name, departure_date, requested_seats, is_active, created_at, last_checked_at
		FROM search_subscriptions
		WHERE is_active = true;
	`

	res, err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) (result.Result, error) {
		return s.Execute(ctx, table.SerializableReadWriteTxControl(), query, nil)
	})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var subs []models.SearchSubscription
	for res.NextResultSet(ctx) {
		for res.Next() {
			var sub models.SearchSubscription
			var lastChecked *time.Time
			if err := res.Scan(&sub.ID, &sub.TelegramChatID, &sub.FromPlaceID, &sub.FromPlaceName,
				&sub.ToPlaceID, &sub.ToPlaceName, &sub.DepartureDate, &sub.RequestedSeats,
				&sub.IsActive, &sub.CreatedAt, &lastChecked); err != nil {
				return nil, err
			}
			sub.LastCheckedAt = lastChecked
			subs = append(subs, sub)
		}
	}

	return subs, nil
}

// UpdateSubscriptionLastChecked updates the last_checked_at timestamp
func UpdateSubscriptionLastChecked(ctx context.Context, subID string) error {
	db, err := GetConnection(ctx)
	if err != nil {
		return err
	}

	query := `
		DECLARE $id AS Utf8;
		DECLARE $last_checked_at AS Timestamp;
		UPDATE search_subscriptions SET last_checked_at = $last_checked_at WHERE id = $id;
	`

	return db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$id", types.UTF8Value(subID)),
				table.ValueParam("$last_checked_at", types.TimestampValueFromTime(time.Now())),
			))
		return err
	})
}

// DeleteSearchSubscription deletes a subscription
func DeleteSearchSubscription(ctx context.Context, subID string) error {
	db, err := GetConnection(ctx)
	if err != nil {
		return err
	}

	query := `
		DECLARE $id AS Utf8;
		DELETE FROM search_subscriptions WHERE id = $id;
	`

	return db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$id", types.UTF8Value(subID)),
			))
		return err
	})
}

// SetSubscriptionActive sets the active status of a subscription
func SetSubscriptionActive(ctx context.Context, subID string, active bool) error {
	db, err := GetConnection(ctx)
	if err != nil {
		return err
	}

	query := `
		DECLARE $id AS Utf8;
		DECLARE $is_active AS Bool;
		UPDATE search_subscriptions SET is_active = $is_active WHERE id = $id;
	`

	return db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$id", types.UTF8Value(subID)),
				table.ValueParam("$is_active", types.BoolValue(active)),
			))
		return err
	})
}

// CreateNotification creates a new notification
func CreateNotification(ctx context.Context, notif *models.Notification) error {
	db, err := GetConnection(ctx)
	if err != nil {
		return err
	}

	if notif.ID == "" {
		notif.ID = uuid.New().String()
	}
	notif.CreatedAt = time.Now()
	notif.Status = "sent"

	query := `
		DECLARE $id AS Utf8;
		DECLARE $telegram_chat_id AS Int64;
		DECLARE $subscription_id AS Utf8;
		DECLARE $trip_id AS Utf8;
		DECLARE $telegram_message_id AS Int32;
		DECLARE $status AS Utf8;
		DECLARE $created_at AS Timestamp;

		INSERT INTO notifications (id, telegram_chat_id, subscription_id, trip_id, telegram_message_id, status, created_at)
		VALUES ($id, $telegram_chat_id, $subscription_id, $trip_id, $telegram_message_id, $status, $created_at);
	`

	return db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$id", types.UTF8Value(notif.ID)),
				table.ValueParam("$telegram_chat_id", types.Int64Value(notif.TelegramChatID)),
				table.ValueParam("$subscription_id", types.UTF8Value(notif.SubscriptionID)),
				table.ValueParam("$trip_id", types.UTF8Value(notif.TripID)),
				table.ValueParam("$telegram_message_id", types.Int32Value(int32(notif.TelegramMessageID))),
				table.ValueParam("$status", types.UTF8Value(notif.Status)),
				table.ValueParam("$created_at", types.TimestampValueFromTime(notif.CreatedAt)),
			))
		return err
	})
}

// GetNotificationByTrip checks if a notification exists for a trip
func GetNotificationByTrip(ctx context.Context, chatID int64, subID, tripID string) (*models.Notification, error) {
	db, err := GetConnection(ctx)
	if err != nil {
		return nil, err
	}

	query := `
		DECLARE $chat_id AS Int64;
		DECLARE $subscription_id AS Utf8;
		DECLARE $trip_id AS Utf8;
		SELECT id, telegram_chat_id, subscription_id, trip_id, telegram_message_id, status, created_at
		FROM notifications
		WHERE telegram_chat_id = $chat_id AND subscription_id = $subscription_id AND trip_id = $trip_id;
	`

	res, err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) (result.Result, error) {
		return s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$chat_id", types.Int64Value(chatID)),
				table.ValueParam("$subscription_id", types.UTF8Value(subID)),
				table.ValueParam("$trip_id", types.UTF8Value(tripID)),
			))
	})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.Next() {
		return nil, nil // No notification found
	}

	var notif models.Notification
	if err := res.Scan(&notif.ID, &notif.TelegramChatID, &notif.SubscriptionID,
		&notif.TripID, &notif.TelegramMessageID, &notif.Status, &notif.CreatedAt); err != nil {
		return nil, err
	}

	return &notif, nil
}

// UpdateNotificationMessageID updates the telegram message ID for a notification
func UpdateNotificationMessageID(ctx context.Context, notifID string, messageID int) error {
	db, err := GetConnection(ctx)
	if err != nil {
		return err
	}

	query := `
		DECLARE $id AS Utf8;
		DECLARE $message_id AS Int32;
		UPDATE notifications SET telegram_message_id = $message_id WHERE id = $id;
	`

	return db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), query,
			table.NewQueryParameters(
				table.ValueParam("$id", types.UTF8Value(notifID)),
				table.ValueParam("$message_id", types.Int32Value(int32(messageID))),
			))
		return err
	})
}
