package models

import "time"

// UserStatus represents the status of a user
type UserStatus string

const (
	UserStatusActive    UserStatus = "active"
	UserStatusInactive  UserStatus = "inactive"
	UserStatusUnauthenticated UserStatus = "unauthenticated"
)

// User represents a bot user
type User struct {
	TelegramChatID       int64      `json:"telegram_chat_id"`
	Status               UserStatus `json:"status"`
	CreatedAt            time.Time  `json:"created_at"`
	LastAuthSuccessAt    *time.Time `json:"last_auth_success_at,omitempty"`
	LastAuthFailureAt    *time.Time `json:"last_auth_failure_at,omitempty"`
}

// UserTokens stores BlaBlaCar authentication tokens
type UserTokens struct {
	TelegramChatID int64     `json:"telegram_chat_id"`
	AccessToken    string    `json:"access_token"`
	RefreshToken   string    `json:"refresh_token"`
	UserID         string    `json:"user_id"`
	Datadome       string    `json:"datadome,omitempty"`
	AppToken       string    `json:"app_token,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// SearchSubscription represents a user's trip search subscription
type SearchSubscription struct {
	ID             string     `json:"id"`
	TelegramChatID int64      `json:"telegram_chat_id"`
	FromPlaceID    string     `json:"from_place_id"`
	FromPlaceName  string     `json:"from_place_name"`
	ToPlaceID      string     `json:"to_place_id"`
	ToPlaceName    string     `json:"to_place_name"`
	DepartureDate  string     `json:"departure_date"`
	RequestedSeats int        `json:"requested_seats"`
	IsActive       bool       `json:"is_active"`
	CreatedAt      time.Time  `json:"created_at"`
	LastCheckedAt  *time.Time `json:"last_checked_at,omitempty"`
}

// TripInfo represents a found trip for notifications
type TripInfo struct {
	ID             string  `json:"id"`
	FromPlaceName  string  `json:"from_place_name"`
	ToPlaceName    string  `json:"to_place_name"`
	DepartureTime  string  `json:"departure_time"`
	ArrivalTime    string  `json:"arrival_time"`
	Duration       string  `json:"duration"`
	Price          string  `json:"price"`
	DriverName     string  `json:"driver_name,omitempty"`
	DriverRating   float64 `json:"driver_rating,omitempty"`
	SeatsAvailable int     `json:"seats_available"`
	IsBus          bool    `json:"is_bus"`
	DeepLink       string  `json:"deep_link"`
}

// Notification represents a trip notification sent to user
type Notification struct {
	ID               string     `json:"id"`
	TelegramChatID   int64      `json:"telegram_chat_id"`
	SubscriptionID   string     `json:"subscription_id"`
	TripID           string     `json:"trip_id"`
	TelegramMessageID int       `json:"telegram_message_id"`
	Status           string     `json:"status"`
	CreatedAt        time.Time  `json:"created_at"`
}
