package ydb

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/arseniisemenow/bbc-common/pkg/models"
	"github.com/flymedllva/ydb-go-qb/yscan"
)

// optionalDatetime creates an optional Datetime value from a uint32 pointer
func optionalDatetime(ts *uint32) types.Value {
	if ts == nil {
		return types.NullValue(types.TypeDatetime)
	}
	return types.OptionalValue(types.DatetimeValue(*ts))
}

// optionalText creates an optional Text value from a string pointer
func optionalText(s *string) types.Value {
	if s == nil {
		return types.NullValue(types.TypeText)
	}
	return types.OptionalValue(types.TextValue(*s))
}

// GetUserByTelegramChatID retrieves a user by their Telegram chat ID
func GetUserByTelegramChatID(ctx context.Context, telegramChatID int64) (*models.User, error) {
	sql := TablePathPrefix("") + `
		DECLARE $telegram_chat_id AS Int64;

		SELECT telegram_chat_id, status, created_at, last_auth_success_at, last_auth_failure_at
		FROM users
		WHERE telegram_chat_id = $telegram_chat_id;
	`

	params := []table.ParameterOption{
		table.ValueParam("$telegram_chat_id", types.Int64Value(telegramChatID)),
	}

	res, err := Query(ctx, sql, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to query user by telegram_chat_id %d: %w", telegramChatID, err)
	}
	defer res.Close()

	log.Printf("[YDB] GetUserByTelegramChatID: Query returned, checking rows...")

	var user models.User
	if res.NextRow() {
		log.Printf("[YDB] GetUserByTelegramChatID: Found row for telegram_chat_id %d", telegramChatID)

		var lastAuthSuccess, lastAuthFailure *uint32
		err = res.Scan(&user.TelegramChatID, &user.Status, &user.CreatedAt, &lastAuthSuccess, &lastAuthFailure)
		if err != nil {
			return nil, fmt.Errorf("failed to scan user: %w", err)
		}
		if lastAuthSuccess != nil {
			t := time.Unix(int64(*lastAuthSuccess), 0)
			user.LastAuthSuccessAt = &t
		}
		if lastAuthFailure != nil {
			t := time.Unix(int64(*lastAuthFailure), 0)
			user.LastAuthFailureAt = &t
		}

		return &user, nil
	}

	log.Printf("[YDB] GetUserByTelegramChatID: No rows found for telegram_chat_id %d", telegramChatID)
	return nil, ErrUserNotFound
}

// UpsertUser inserts or updates a user
func UpsertUser(ctx context.Context, user *models.User) error {
	sql := TablePathPrefix("") + `
		DECLARE $telegram_chat_id AS Int64;
		DECLARE $status AS Utf8;
		DECLARE $created_at AS Datetime;
		DECLARE $last_auth_success_at AS Optional<Datetime>;
		DECLARE $last_auth_failure_at AS Optional<Datetime>;

		UPSERT INTO users (telegram_chat_id, status, created_at, last_auth_success_at, last_auth_failure_at)
		VALUES ($telegram_chat_id, $status, $created_at, $last_auth_success_at, $last_auth_failure_at);
	`

	var lastAuthSuccess, lastAuthFailure *uint32
	if user.LastAuthSuccessAt != nil {
		t := uint32(user.LastAuthSuccessAt.Unix())
		lastAuthSuccess = &t
	}
	if user.LastAuthFailureAt != nil {
		t := uint32(user.LastAuthFailureAt.Unix())
		lastAuthFailure = &t
	}

	params := []table.ParameterOption{
		table.ValueParam("$telegram_chat_id", types.Int64Value(user.TelegramChatID)),
		table.ValueParam("$status", types.TextValue(string(user.Status))),
		table.ValueParam("$created_at", types.DatetimeValue(uint32(user.CreatedAt.Unix()))),
		table.ValueParam("$last_auth_success_at", optionalDatetime(lastAuthSuccess)),
		table.ValueParam("$last_auth_failure_at", optionalDatetime(lastAuthFailure)),
	}

	log.Printf("[YDB] UpsertUser: Attempting to upsert user with telegram_chat_id %d", user.TelegramChatID)
	return Exec(ctx, sql, params...)
}

// UpdateUserStatus updates a user's status
func UpdateUserStatus(ctx context.Context, chatID int64, status models.UserStatus) error {
	sql := TablePathPrefix("") + `
		DECLARE $telegram_chat_id AS Int64;
		DECLARE $status AS Utf8;

		UPDATE users
		SET status = $status
		WHERE telegram_chat_id = $telegram_chat_id;
	`

	params := []table.ParameterOption{
		table.ValueParam("$telegram_chat_id", types.Int64Value(chatID)),
		table.ValueParam("$status", types.TextValue(string(status))),
	}

	return Exec(ctx, sql, params...)
}

// GetActiveUsers retrieves all active users
func GetActiveUsers(ctx context.Context) ([]models.User, error) {
	sql := TablePathPrefix("") + `
		SELECT telegram_chat_id, status, created_at, last_auth_success_at, last_auth_failure_at
		FROM users
		WHERE status = "active";
	`

	res, err := Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to query active users: %w", err)
	}
	defer res.Close()

	var users []models.User
	for res.NextRow() {
		var user models.User
		var lastAuthSuccess, lastAuthFailure *uint32
		err = res.Scan(&user.TelegramChatID, &user.Status, &user.CreatedAt, &lastAuthSuccess, &lastAuthFailure)
		if err != nil {
			return nil, fmt.Errorf("failed to scan user: %w", err)
		}
		if lastAuthSuccess != nil {
			t := time.Unix(int64(*lastAuthSuccess), 0)
			user.LastAuthSuccessAt = &t
		}
		if lastAuthFailure != nil {
			t := time.Unix(int64(*lastAuthFailure), 0)
			user.LastAuthFailureAt = &t
		}
		users = append(users, user)
	}

	return users, nil
}

// GetUserTokens retrieves tokens for a user
func GetUserTokens(ctx context.Context, chatID int64) (*models.UserTokens, error) {
	log.Printf("[YDB] GetUserTokens: searching for chatID=%d", chatID)

	sql := TablePathPrefix("") + `
		DECLARE $telegram_chat_id AS Int64;

		SELECT telegram_chat_id, access_token, refresh_token, user_id, datadome, app_token, created_at, updated_at
		FROM user_tokens
		WHERE telegram_chat_id = $telegram_chat_id;
	`

	params := []table.ParameterOption{
		table.ValueParam("$telegram_chat_id", types.Int64Value(chatID)),
	}

	res, err := Query(ctx, sql, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to query user tokens: %w", err)
	}
	defer res.Close()

	if res.NextRow() {
		log.Printf("[YDB] GetUserTokens: found row for chatID=%d", chatID)
		var tokens models.UserTokens
		err = yscan.ScanRow(&tokens, res)
		if err != nil {
			return nil, fmt.Errorf("failed to scan user tokens: %w", err)
		}
		return &tokens, nil
	}

	log.Printf("[YDB] GetUserTokens: no row found for chatID=%d, returning ErrTokensNotFound", chatID)
	return nil, ErrTokensNotFound
}

// StoreUserTokens stores or updates user tokens
func StoreUserTokens(ctx context.Context, tokens *models.UserTokens) error {
	log.Printf("[YDB] StoreUserTokens: storing tokens for chatID=%d, userID=%s", tokens.TelegramChatID, tokens.UserID)

	sql := TablePathPrefix("") + `
		DECLARE $telegram_chat_id AS Int64;
		DECLARE $access_token AS Utf8;
		DECLARE $refresh_token AS Utf8;
		DECLARE $user_id AS Utf8;
		DECLARE $datadome AS Optional<Utf8>;
		DECLARE $app_token AS Optional<Utf8>;
		DECLARE $created_at AS Datetime;
		DECLARE $updated_at AS Datetime;

		UPSERT INTO user_tokens (telegram_chat_id, access_token, refresh_token, user_id, datadome, app_token, created_at, updated_at)
		VALUES ($telegram_chat_id, $access_token, $refresh_token, $user_id, $datadome, $app_token, $created_at, $updated_at);
	`

	var datadome, appToken *string
	if tokens.Datadome != "" {
		datadome = &tokens.Datadome
	}
	if tokens.AppToken != "" {
		appToken = &tokens.AppToken
	}

	params := []table.ParameterOption{
		table.ValueParam("$telegram_chat_id", types.Int64Value(tokens.TelegramChatID)),
		table.ValueParam("$access_token", types.TextValue(tokens.AccessToken)),
		table.ValueParam("$refresh_token", types.TextValue(tokens.RefreshToken)),
		table.ValueParam("$user_id", types.TextValue(tokens.UserID)),
		table.ValueParam("$datadome", optionalText(datadome)),
		table.ValueParam("$app_token", optionalText(appToken)),
		table.ValueParam("$created_at", types.DatetimeValue(uint32(tokens.CreatedAt.Unix()))),
		table.ValueParam("$updated_at", types.DatetimeValue(uint32(tokens.UpdatedAt.Unix()))),
	}

	return Exec(ctx, sql, params...)
}

// DeleteUserTokens removes tokens for a user
func DeleteUserTokens(ctx context.Context, chatID int64) error {
	sql := TablePathPrefix("") + `
		DECLARE $telegram_chat_id AS Int64;

		DELETE FROM user_tokens
		WHERE telegram_chat_id = $telegram_chat_id;
	`

	params := []table.ParameterOption{
		table.ValueParam("$telegram_chat_id", types.Int64Value(chatID)),
	}

	return Exec(ctx, sql, params...)
}

// CreateSearchSubscription creates a new search subscription
func CreateSearchSubscription(ctx context.Context, sub *models.SearchSubscription) error {
	sql := TablePathPrefix("") + `
		DECLARE $id AS Utf8;
		DECLARE $telegram_chat_id AS Int64;
		DECLARE $from_place_id AS Utf8;
		DECLARE $from_place_name AS Utf8;
		DECLARE $to_place_id AS Utf8;
		DECLARE $to_place_name AS Utf8;
		DECLARE $departure_date AS Utf8;
		DECLARE $requested_seats AS Int32;
		DECLARE $is_active AS Bool;
		DECLARE $created_at AS Datetime;

		INSERT INTO search_subscriptions (id, telegram_chat_id, from_place_id, from_place_name, to_place_id, to_place_name, departure_date, requested_seats, is_active, created_at)
		VALUES ($id, $telegram_chat_id, $from_place_id, $from_place_name, $to_place_id, $to_place_name, $departure_date, $requested_seats, $is_active, $created_at);
	`

	params := []table.ParameterOption{
		table.ValueParam("$id", types.TextValue(sub.ID)),
		table.ValueParam("$telegram_chat_id", types.Int64Value(sub.TelegramChatID)),
		table.ValueParam("$from_place_id", types.TextValue(sub.FromPlaceID)),
		table.ValueParam("$from_place_name", types.TextValue(sub.FromPlaceName)),
		table.ValueParam("$to_place_id", types.TextValue(sub.ToPlaceID)),
		table.ValueParam("$to_place_name", types.TextValue(sub.ToPlaceName)),
		table.ValueParam("$departure_date", types.TextValue(sub.DepartureDate)),
		table.ValueParam("$requested_seats", types.Int32Value(int32(sub.RequestedSeats))),
		table.ValueParam("$is_active", types.BoolValue(sub.IsActive)),
		table.ValueParam("$created_at", types.DatetimeValue(uint32(sub.CreatedAt.Unix()))),
	}

	return Exec(ctx, sql, params...)
}

// GetSearchSubscriptionsByUser retrieves all subscriptions for a user
func GetSearchSubscriptionsByUser(ctx context.Context, chatID int64) ([]models.SearchSubscription, error) {
	sql := TablePathPrefix("") + `
		DECLARE $telegram_chat_id AS Int64;

		SELECT id, telegram_chat_id, from_place_id, from_place_name, to_place_id, to_place_name, departure_date, requested_seats, is_active, created_at, last_checked_at
		FROM search_subscriptions
		WHERE telegram_chat_id = $telegram_chat_id;
	`

	params := []table.ParameterOption{
		table.ValueParam("$telegram_chat_id", types.Int64Value(chatID)),
	}

	res, err := Query(ctx, sql, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to query subscriptions: %w", err)
	}
	defer res.Close()

	var subs []models.SearchSubscription
	for res.NextRow() {
		var sub models.SearchSubscription
		var lastChecked *uint32
		err = res.Scan(&sub.ID, &sub.TelegramChatID, &sub.FromPlaceID, &sub.FromPlaceName,
			&sub.ToPlaceID, &sub.ToPlaceName, &sub.DepartureDate, &sub.RequestedSeats,
			&sub.IsActive, &sub.CreatedAt, &lastChecked)
		if err != nil {
			return nil, fmt.Errorf("failed to scan subscription: %w", err)
		}
		if lastChecked != nil {
			t := time.Unix(int64(*lastChecked), 0)
			sub.LastCheckedAt = &t
		}
		subs = append(subs, sub)
	}

	return subs, nil
}

// GetActiveSubscriptions retrieves all active subscriptions
func GetActiveSubscriptions(ctx context.Context) ([]models.SearchSubscription, error) {
	sql := TablePathPrefix("") + `
		SELECT id, telegram_chat_id, from_place_id, from_place_name, to_place_id, to_place_name, departure_date, requested_seats, is_active, created_at, last_checked_at
		FROM search_subscriptions
		WHERE is_active = true;
	`

	res, err := Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to query active subscriptions: %w", err)
	}
	defer res.Close()

	var subs []models.SearchSubscription
	for res.NextRow() {
		var sub models.SearchSubscription
		var lastChecked *uint32
		err = res.Scan(&sub.ID, &sub.TelegramChatID, &sub.FromPlaceID, &sub.FromPlaceName,
			&sub.ToPlaceID, &sub.ToPlaceName, &sub.DepartureDate, &sub.RequestedSeats,
			&sub.IsActive, &sub.CreatedAt, &lastChecked)
		if err != nil {
			return nil, fmt.Errorf("failed to scan subscription: %w", err)
		}
		if lastChecked != nil {
			t := time.Unix(int64(*lastChecked), 0)
			sub.LastCheckedAt = &t
		}
		subs = append(subs, sub)
	}

	return subs, nil
}

// UpdateSubscriptionLastChecked updates the last_checked_at timestamp
func UpdateSubscriptionLastChecked(ctx context.Context, subID string) error {
	sql := TablePathPrefix("") + `
		DECLARE $id AS Utf8;
		DECLARE $last_checked_at AS Datetime;

		UPDATE search_subscriptions SET last_checked_at = $last_checked_at WHERE id = $id;
	`

	params := []table.ParameterOption{
		table.ValueParam("$id", types.TextValue(subID)),
		table.ValueParam("$last_checked_at", types.DatetimeValue(uint32(time.Now().Unix()))),
	}

	return Exec(ctx, sql, params...)
}

// DeleteSearchSubscription deletes a subscription
func DeleteSearchSubscription(ctx context.Context, subID string) error {
	sql := TablePathPrefix("") + `
		DECLARE $id AS Utf8;

		DELETE FROM search_subscriptions WHERE id = $id;
	`

	params := []table.ParameterOption{
		table.ValueParam("$id", types.TextValue(subID)),
	}

	return Exec(ctx, sql, params...)
}

// SetSubscriptionActive sets the active status of a subscription
func SetSubscriptionActive(ctx context.Context, subID string, active bool) error {
	sql := TablePathPrefix("") + `
		DECLARE $id AS Utf8;
		DECLARE $is_active AS Bool;

		UPDATE search_subscriptions SET is_active = $is_active WHERE id = $id;
	`

	params := []table.ParameterOption{
		table.ValueParam("$id", types.TextValue(subID)),
		table.ValueParam("$is_active", types.BoolValue(active)),
	}

	return Exec(ctx, sql, params...)
}

// CreateNotification creates a new notification
func CreateNotification(ctx context.Context, notif *models.Notification) error {
	sql := TablePathPrefix("") + `
		DECLARE $id AS Utf8;
		DECLARE $telegram_chat_id AS Int64;
		DECLARE $subscription_id AS Utf8;
		DECLARE $trip_id AS Utf8;
		DECLARE $telegram_message_id AS Int32;
		DECLARE $status AS Utf8;
		DECLARE $created_at AS Datetime;

		INSERT INTO notifications (id, telegram_chat_id, subscription_id, trip_id, telegram_message_id, status, created_at)
		VALUES ($id, $telegram_chat_id, $subscription_id, $trip_id, $telegram_message_id, $status, $created_at);
	`

	params := []table.ParameterOption{
		table.ValueParam("$id", types.TextValue(notif.ID)),
		table.ValueParam("$telegram_chat_id", types.Int64Value(notif.TelegramChatID)),
		table.ValueParam("$subscription_id", types.TextValue(notif.SubscriptionID)),
		table.ValueParam("$trip_id", types.TextValue(notif.TripID)),
		table.ValueParam("$telegram_message_id", types.Int32Value(int32(notif.TelegramMessageID))),
		table.ValueParam("$status", types.TextValue(notif.Status)),
		table.ValueParam("$created_at", types.DatetimeValue(uint32(notif.CreatedAt.Unix()))),
	}

	return Exec(ctx, sql, params...)
}

// GetNotificationByTrip checks if a notification exists for a trip
func GetNotificationByTrip(ctx context.Context, chatID int64, subID, tripID string) (*models.Notification, error) {
	sql := TablePathPrefix("") + `
		DECLARE $telegram_chat_id AS Int64;
		DECLARE $subscription_id AS Utf8;
		DECLARE $trip_id AS Utf8;

		SELECT id, telegram_chat_id, subscription_id, trip_id, telegram_message_id, status, created_at
		FROM notifications
		WHERE telegram_chat_id = $telegram_chat_id AND subscription_id = $subscription_id AND trip_id = $trip_id;
	`

	params := []table.ParameterOption{
		table.ValueParam("$telegram_chat_id", types.Int64Value(chatID)),
		table.ValueParam("$subscription_id", types.TextValue(subID)),
		table.ValueParam("$trip_id", types.TextValue(tripID)),
	}

	res, err := Query(ctx, sql, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to query notification: %w", err)
	}
	defer res.Close()

	if res.NextRow() {
		var notif models.Notification
		var createdAt uint32
		err = res.Scan(&notif.ID, &notif.TelegramChatID, &notif.SubscriptionID,
			&notif.TripID, &notif.TelegramMessageID, &notif.Status, &createdAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan notification: %w", err)
		}
		notif.CreatedAt = time.Unix(int64(createdAt), 0)
		return &notif, nil
	}

	return nil, nil // No notification found
}

// UpdateNotificationMessageID updates the telegram message ID for a notification
func UpdateNotificationMessageID(ctx context.Context, notifID string, messageID int) error {
	sql := TablePathPrefix("") + `
		DECLARE $id AS Utf8;
		DECLARE $telegram_message_id AS Int32;

		UPDATE notifications SET telegram_message_id = $telegram_message_id WHERE id = $id;
	`

	params := []table.ParameterOption{
		table.ValueParam("$id", types.TextValue(notifID)),
		table.ValueParam("$telegram_message_id", types.Int32Value(int32(messageID))),
	}

	return Exec(ctx, sql, params...)
}
