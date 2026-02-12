package telegram

// BotSender defines the interface for sending Telegram messages
type BotSender interface {
	SendPlainMessage(chatID int64, text string) error
	SendMessageWithKeyboard(chatID int64, text string, keyboard interface{}) (int, error)
	EditMessage(chatID int64, messageID int, text string) error
	AnswerCallbackQuery(callbackQueryID, text string) error
}
