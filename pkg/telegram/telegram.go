package telegram

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	tba "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// BotClient wraps the Telegram bot API
type BotClient struct {
	bot *tba.BotAPI
}

// NewBotClientFromEnv creates a new bot client from environment variable
func NewBotClientFromEnv() (*BotClient, error) {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	if token == "" {
		return nil, fmt.Errorf("TELEGRAM_BOT_TOKEN not set")
	}

	bot, err := tba.NewBotAPI(token)
	if err != nil {
		return nil, fmt.Errorf("failed to create bot: %w", err)
	}

	return &BotClient{bot: bot}, nil
}

// SendPlainMessage sends a simple text message
func (bc *BotClient) SendPlainMessage(chatID int64, text string) error {
	escapedText := tba.EscapeText(tba.ModeMarkdownV2, text)

	msg := tba.NewMessage(chatID, escapedText)
	msg.ParseMode = "MarkdownV2"

	_, err := bc.bot.Send(msg)
	return err
}

// SendMessageWithKeyboard sends a message with an inline keyboard
func (bc *BotClient) SendMessageWithKeyboard(chatID int64, text string, keyboard interface{}) (int, error) {
	escapedText := tba.EscapeText(tba.ModeMarkdownV2, text)

	msg := tba.NewMessage(chatID, escapedText)
	msg.ParseMode = "MarkdownV2"
	msg.ReplyMarkup = keyboard

	sent, err := bc.bot.Send(msg)
	if err != nil {
		return 0, err
	}
	return sent.MessageID, nil
}

// EditMessage edits an existing message
func (bc *BotClient) EditMessage(chatID int64, messageID int, text string) error {
	escapedText := tba.EscapeText(tba.ModeMarkdownV2, text)

	msg := tba.NewEditMessageText(chatID, messageID, escapedText)
	msg.ParseMode = "MarkdownV2"

	_, err := bc.bot.Send(msg)
	return err
}

// AnswerCallbackQuery answers a callback query
func (bc *BotClient) AnswerCallbackQuery(callbackQueryID, text string) error {
	callback := tba.NewCallback(callbackQueryID, text)
	_, err := bc.bot.Request(callback)
	return err
}

// SendInlineKeyboard sends a message with inline buttons
func (bc *BotClient) SendInlineKeyboard(chatID int64, text string, buttons [][]tba.InlineKeyboardButton) (int, error) {
	escapedText := tba.EscapeText(tba.ModeMarkdownV2, text)

	msg := tba.NewMessage(chatID, escapedText)
	msg.ParseMode = "MarkdownV2"
	msg.ReplyMarkup = tba.NewInlineKeyboardMarkup(buttons...)

	sent, err := bc.bot.Send(msg)
	if err != nil {
		return 0, err
	}
	return sent.MessageID, nil
}

// FormatTripMessage formats a trip notification message
func FormatTripMessage(trip interface{}) string {
	// This will be implemented based on the trip structure
	return ""
}

// ParseCallbackData parses callback data in format "action:param1:param2"
func ParseCallbackData(data string) (action string, params []string) {
	parts := strings.Split(data, ":")
	if len(parts) == 0 {
		return "", nil
	}
	return parts[0], parts[1:]
}

// CreateCallbackData creates callback data in format "action:param1:param2"
func CreateCallbackData(action string, params ...string) string {
	return strings.Join(append([]string{action}, params...), ":")
}

// FormatSubscriptionMessage formats a subscription for display
func FormatSubscriptionMessage(id, from, to, date string, isActive bool) string {
	status := "✅ Active"
	if !isActive {
		status = "❌ Inactive"
	}
	return fmt.Sprintf("*Subscription #%s*\n%s → %s\nDate: %s\nStatus: %s",
		id[:8], from, to, date, status)
}

// FormatSubscriptionsList formats a list of subscriptions
func FormatSubscriptionsList(subscriptions []string) string {
	if len(subscriptions) == 0 {
		return "No active subscriptions"
	}
	return strings.Join(subscriptions, "\n\n")
}

// GetChatIDFromString converts string chat ID to int64
func GetChatIDFromString(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
