// Reaction-as-feedback: when a human user reacts to one of the bot's
// messages (👍/👎/❤️/...), forward the reaction as a user message to
// Claude so it appears in the conversation as feedback on the previous
// reply. Reactions added by the bot itself (its own "OnIt" typing
// indicator) are filtered out by OperatorType.
package feishu

import (
	"context"
	"fmt"

	"github.com/chenhg5/cc-connect/core"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// reactionEmojiDisplay maps Feishu's emoji type identifiers to rendered
// characters. Falls through to the raw type name when unknown so Claude
// still sees something identifiable.
var reactionEmojiDisplay = map[string]string{
	"THUMBSUP":   "👍",
	"THUMBSDOWN": "👎",
	"HEART":      "❤️",
	"FIRE":       "🔥",
	"SMILE":      "😄",
	"LAUGH":      "😂",
	"CRY":        "😢",
	"ANGRY":      "😠",
	"SURPRISE":   "😮",
	"CLAP":       "👏",
	"PRAY":       "🙏",
	"OK":         "👌",
	"DONE":       "✅",
}

// onReactionCreated filters a reaction-created event down to genuine user
// feedback on the bot's messages and forwards it as a text user message.
func (p *Platform) onReactionCreated(event *larkim.P2MessageReactionCreatedV1) error {
	if event == nil || event.Event == nil {
		return nil
	}
	ev := event.Event

	// Skip reactions the bot itself added (the "OnIt" typing indicator).
	if ev.OperatorType == nil || *ev.OperatorType != "user" {
		return nil
	}
	if ev.UserId == nil || ev.UserId.OpenId == nil || *ev.UserId.OpenId == "" {
		return nil
	}
	userID := *ev.UserId.OpenId
	if ev.MessageId == nil || *ev.MessageId == "" {
		return nil
	}
	messageID := *ev.MessageId

	// Look up chat_id and sender_id for the reacted-to message to confirm
	// it's a bot reply (sender is this app) and to route to the correct session.
	chatID, senderID, err := p.getMessageChatAndSender(context.Background(), messageID)
	if err != nil {
		return nil // silently skip if lookup fails
	}
	// Only forward reactions on OUR messages — user reacting on their own
	// or someone else's message isn't feedback for Claude.
	if senderID != "" && senderID != p.appID {
		return nil
	}

	emoji := ""
	if ev.ReactionType != nil && ev.ReactionType.EmojiType != nil {
		emoji = *ev.ReactionType.EmojiType
	}
	display := reactionEmojiDisplay[emoji]
	if display == "" {
		display = emoji
	}

	sessionKey := fmt.Sprintf("feishu:%s:%s", chatID, userID)
	rctx := replyContext{messageID: messageID, chatID: chatID, sessionKey: sessionKey}

	content := fmt.Sprintf("[feedback] %s (reaction on your previous reply)", display)
	go p.handler(p.dispatchPlatform(), &core.Message{
		SessionKey: sessionKey,
		Platform:   p.platformName,
		UserID:     userID,
		UserName:   p.resolveUserName(userID),
		ChatName:   p.resolveChatName(chatID),
		Content:    content,
		ReplyCtx:   rctx,
	})
	return nil
}

// getMessageChatAndSender queries Im.Message.Get for a message and returns
// (chat_id, sender_id, err). sender_id is the app_id when the message was
// sent by a bot and the user's open_id when sent by a human.
func (p *Platform) getMessageChatAndSender(ctx context.Context, messageID string) (string, string, error) {
	req := larkim.NewGetMessageReqBuilder().MessageId(messageID).Build()
	var resp *larkim.GetMessageResp
	if err := p.withTransientRetry(ctx, "reaction lookup", func() error {
		return p.withFreshTenantAccessTokenRetry(ctx, "reaction lookup", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
			var err error
			resp, err = client.Im.Message.Get(ctx, req, options...)
			if err != nil {
				return err
			}
			if !resp.Success() {
				return fmt.Errorf("code=%d msg=%s", resp.Code, resp.Msg)
			}
			return nil
		})
	}); err != nil {
		return "", "", err
	}
	if resp.Data == nil || len(resp.Data.Items) == 0 {
		return "", "", fmt.Errorf("no items")
	}
	item := resp.Data.Items[0]
	chatID := ""
	if item.ChatId != nil {
		chatID = *item.ChatId
	}
	senderID := ""
	if item.Sender != nil && item.Sender.Id != nil {
		senderID = *item.Sender.Id
	}
	return chatID, senderID, nil
}
