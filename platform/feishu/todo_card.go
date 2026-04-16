// Todo list rendering as a live-updating Feishu card.
//
// Claude's TodoWrite tool emits the full current list on every call. We
// keep a single card per (chatID, userID) — created on the first update,
// patched in place on each subsequent update via Im.Message.Patch. The
// user sees tasks tick through pending → in_progress → completed without
// the chat being flooded with repeated snapshots.
//
// CardKit streaming isn't used here because the content is structured
// (checkboxes with per-item formatting), not plain text that benefits
// from typewriter animation. Standard interactive card + patch suffices.
package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/chenhg5/cc-connect/core"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// todoCardRegistry maps a session key (chatID:userID) to its live todo
// card's message ID. One card per session; reused across TodoWrite updates.
var todoCardRegistry = struct {
	sync.Mutex
	cards map[string]string // sessionKey → messageID
}{cards: map[string]string{}}

func todoCardKey(chatID, userID string) string {
	return chatID + ":" + userID
}

// RenderTodoList creates or updates a Feishu card showing Claude's current
// TodoWrite list. Implements core.TodoListRenderer.
func (p *Platform) RenderTodoList(ctx context.Context, rctx any, todos []core.TodoItem) error {
	if !p.useInteractiveCard {
		return core.ErrNotSupported
	}
	rc, ok := rctx.(replyContext)
	if !ok {
		return fmt.Errorf("%s: RenderTodoList: invalid reply context type %T", p.tag(), rctx)
	}
	if rc.chatID == "" {
		return fmt.Errorf("%s: RenderTodoList: empty chatID", p.tag())
	}

	cardJSON := buildTodoCardJSON(todos)
	key := todoCardKey(rc.chatID, rc.sessionKey)

	// If a todo card already exists for this session, patch it.
	todoCardRegistry.Lock()
	existingID := todoCardRegistry.cards[key]
	todoCardRegistry.Unlock()

	if existingID != "" {
		if err := p.patchTodoCard(ctx, existingID, cardJSON); err == nil {
			return nil
		}
		// Patch failed (message deleted, etc.) — drop the stale entry and
		// fall through to create a fresh card.
		todoCardRegistry.Lock()
		delete(todoCardRegistry.cards, key)
		todoCardRegistry.Unlock()
	}

	// Fresh card.
	msgID, err := p.sendNewTodoCard(ctx, rc, cardJSON)
	if err != nil {
		return err
	}
	todoCardRegistry.Lock()
	todoCardRegistry.cards[key] = msgID
	todoCardRegistry.Unlock()
	return nil
}

// buildTodoCardJSON renders a Feishu interactive card from the todo items.
// Uses checkbox-style emoji icons and groups items visually:
//
//	✅ completed   (strikethrough-like grey)
//	🔄 in_progress (bold, blue)
//	☐  pending    (plain)
func buildTodoCardJSON(todos []core.TodoItem) string {
	completed, inProgress, pending := 0, 0, 0
	var sb strings.Builder
	for _, t := range todos {
		text := strings.TrimSpace(t.Content)
		if text == "" {
			continue
		}
		// Escape backticks so markdown inline code doesn't break layout.
		text = strings.ReplaceAll(text, "`", "'")
		switch strings.ToLower(strings.TrimSpace(t.Status)) {
		case "completed":
			sb.WriteString("✅ <font color=\"grey\">~~")
			sb.WriteString(text)
			sb.WriteString("~~</font>\n")
			completed++
		case "in_progress":
			sb.WriteString("🔄 **")
			if t.ActiveForm != "" {
				sb.WriteString(strings.ReplaceAll(t.ActiveForm, "`", "'"))
			} else {
				sb.WriteString(text)
			}
			sb.WriteString("**\n")
			inProgress++
		default: // pending or unknown
			sb.WriteString("☐ ")
			sb.WriteString(text)
			sb.WriteString("\n")
			pending++
		}
	}
	body := strings.TrimRight(sb.String(), "\n")
	if body == "" {
		body = "_(empty list)_"
	}

	total := completed + inProgress + pending
	header := fmt.Sprintf("📋 Todo  ·  %d/%d done", completed, total)
	if inProgress > 0 {
		header += fmt.Sprintf("  ·  %d in progress", inProgress)
	}

	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{"wide_screen_mode": true},
		"header": map[string]any{
			"title":    map[string]any{"tag": "plain_text", "content": header},
			"template": headerTemplateForTodo(completed, total, inProgress),
		},
		"body": map[string]any{
			"elements": []any{
				map[string]any{
					"tag":       "markdown",
					"content":   body,
					"text_size": "normal_v2",
				},
			},
		},
	}
	b, _ := json.Marshal(card)
	return string(b)
}

func headerTemplateForTodo(completed, total, inProgress int) string {
	if total == 0 {
		return "grey"
	}
	if completed == total {
		return "green"
	}
	if inProgress > 0 {
		return "blue"
	}
	return "wathet"
}

// sendNewTodoCard posts a fresh todo card as a reply in the current chat.
// Returns the resulting message_id for future in-place patches.
func (p *Platform) sendNewTodoCard(ctx context.Context, rc replyContext, cardJSON string) (string, error) {
	if p.shouldUseThreadOrReplyAPI(rc) {
		req := larkim.NewReplyMessageReqBuilder().
			MessageId(rc.messageID).
			Body(p.buildReplyMessageReqBody(rc, larkim.MsgTypeInteractive, cardJSON)).
			Build()
		var resp *larkim.ReplyMessageResp
		if err := p.withTransientRetry(ctx, "send todo card (reply)", func() error {
			return p.withFreshTenantAccessTokenRetry(ctx, "send todo card (reply)", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
				var err error
				resp, err = client.Im.Message.Reply(ctx, req, options...)
				if err != nil {
					return fmt.Errorf("%s: send todo card: %w", p.tag(), err)
				}
				if !resp.Success() {
					return fmt.Errorf("%s: send todo card code=%d msg=%s", p.tag(), resp.Code, resp.Msg)
				}
				return nil
			})
		}); err != nil {
			return "", err
		}
		if resp.Data != nil && resp.Data.MessageId != nil {
			return *resp.Data.MessageId, nil
		}
		return "", fmt.Errorf("%s: send todo card: no message_id", p.tag())
	}

	req := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(larkim.ReceiveIdTypeChatId).
		Body(larkim.NewCreateMessageReqBodyBuilder().
			ReceiveId(rc.chatID).
			MsgType(larkim.MsgTypeInteractive).
			Content(cardJSON).
			Build()).
		Build()
	var resp *larkim.CreateMessageResp
	if err := p.withTransientRetry(ctx, "send todo card", func() error {
		return p.withFreshTenantAccessTokenRetry(ctx, "send todo card", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
			var err error
			resp, err = client.Im.Message.Create(ctx, req, options...)
			if err != nil {
				return fmt.Errorf("%s: send todo card: %w", p.tag(), err)
			}
			if !resp.Success() {
				return fmt.Errorf("%s: send todo card code=%d msg=%s", p.tag(), resp.Code, resp.Msg)
			}
			return nil
		})
	}); err != nil {
		return "", err
	}
	if resp.Data != nil && resp.Data.MessageId != nil {
		return *resp.Data.MessageId, nil
	}
	return "", fmt.Errorf("%s: send todo card: no message_id", p.tag())
}

// patchTodoCard updates an existing todo card in place.
func (p *Platform) patchTodoCard(ctx context.Context, messageID, cardJSON string) error {
	req := larkim.NewPatchMessageReqBuilder().
		MessageId(messageID).
		Body(larkim.NewPatchMessageReqBodyBuilder().
			Content(cardJSON).
			Build()).
		Build()
	return p.withTransientRetry(ctx, "patch todo card", func() error {
		return p.withFreshTenantAccessTokenRetry(ctx, "patch todo card", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
			resp, err := client.Im.Message.Patch(ctx, req, options...)
			if err != nil {
				return fmt.Errorf("%s: patch todo card: %w", p.tag(), err)
			}
			if !resp.Success() {
				return fmt.Errorf("%s: patch todo card code=%d msg=%s", p.tag(), resp.Code, resp.Msg)
			}
			return nil
		})
	})
}
