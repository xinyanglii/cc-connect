// Package feishu — streaming card implementation using Feishu's CardKit API.
//
// Feishu's CardKit service renders in-place text updates with a client-side
// typewriter animation when the card's config has streaming_mode=true. This
// gives the same UX as the Feishu AI Assistant (Doubao) — characters
// appearing one-by-one rather than whole-block replacements.
//
// Flow:
//  1. cardkit.Card.Create    → receive card_id
//  2. Im.Message.Create/Reply → attach to chat (content ref'd by card_id)
//  3. cardkit.CardElement.Content (N times, monotonically increasing
//     sequence) → push cumulative text updates; client animates diff
//  4. cardkit.Card.Settings (streaming_mode=false) → close streaming;
//     card becomes interactive/forwardable normally
//
// Sequence numbers must strictly increase during a streaming window; Feishu
// returns an error if a smaller sequence is received after a larger one.
package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkcardkit "github.com/larksuite/oapi-sdk-go/v3/service/cardkit/v1"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// streamingElementID is the fixed ID of the markdown element that receives
// streamed text. Must match what buildStreamingCardJSON produces.
const streamingElementID = "streaming_content"

// feishuStreamingHandle is returned from SendPreviewStart when streaming
// cards are enabled. Carries the cardkit card_id (needed for subsequent
// cardElement.Content calls), the IM message_id (for fallback and logging),
// and a monotonic sequence counter.
type feishuStreamingHandle struct {
	cardID    string
	messageID string
	chatID    string
	seq       atomic.Int64 // starts at 0; Add(1) before each streaming call
}

// nextSeq returns the next monotonic sequence number.
func (h *feishuStreamingHandle) nextSeq() int {
	return int(h.seq.Add(1))
}

// createCardKitCard creates a card entity via CardKit and returns its ID.
// The card_id is then attached to an IM message via Im.Message.Create.
func (p *Platform) createCardKitCard(ctx context.Context, cardJSON string) (string, error) {
	req := larkcardkit.NewCreateCardReqBuilder().
		Body(larkcardkit.NewCreateCardReqBodyBuilder().
			Type("card_json").
			Data(cardJSON).
			Build()).
		Build()
	var resp *larkcardkit.CreateCardResp
	if err := p.withTransientRetry(ctx, "cardkit card create", func() error {
		return p.withFreshTenantAccessTokenRetry(ctx, "cardkit card create", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
			var err error
			resp, err = client.Cardkit.V1.Card.Create(ctx, req, options...)
			if err != nil {
				return fmt.Errorf("%s: cardkit card create: %w", p.tag(), err)
			}
			if !resp.Success() {
				return fmt.Errorf("%s: cardkit card create code=%d msg=%s", p.tag(), resp.Code, resp.Msg)
			}
			return nil
		})
	}); err != nil {
		return "", err
	}
	if resp.Data == nil || resp.Data.CardId == nil || *resp.Data.CardId == "" {
		return "", fmt.Errorf("%s: cardkit card create: no card_id returned", p.tag())
	}
	return *resp.Data.CardId, nil
}

// streamCardContent pushes a cumulative text update to the streaming element.
// Content is the full accumulated text; the Feishu client diffs it against
// the previous and renders the incremental part with a typewriter animation.
func (p *Platform) streamCardContent(ctx context.Context, cardID, elementID, content string, sequence int) error {
	req := larkcardkit.NewContentCardElementReqBuilder().
		CardId(cardID).
		ElementId(elementID).
		Body(larkcardkit.NewContentCardElementReqBodyBuilder().
			Content(content).
			Sequence(sequence).
			Build()).
		Build()
	return p.withTransientRetry(ctx, "cardkit element content", func() error {
		return p.withFreshTenantAccessTokenRetry(ctx, "cardkit element content", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
			resp, err := client.Cardkit.V1.CardElement.Content(ctx, req, options...)
			if err != nil {
				return fmt.Errorf("%s: cardkit element content: %w", p.tag(), err)
			}
			if !resp.Success() {
				return fmt.Errorf("%s: cardkit element content code=%d msg=%s seq=%d", p.tag(), resp.Code, resp.Msg, sequence)
			}
			return nil
		})
	})
}

// setCardStreamingMode toggles the card's streaming_mode. After streaming
// is complete, set it to false so the card renders as a normal interactive
// card (forwardable, action buttons respond, etc.).
func (p *Platform) setCardStreamingMode(ctx context.Context, cardID string, streamingMode bool, sequence int) error {
	settings, err := json.Marshal(map[string]any{"config": map[string]any{"streaming_mode": streamingMode}})
	if err != nil {
		return fmt.Errorf("%s: marshal streaming settings: %w", p.tag(), err)
	}
	req := larkcardkit.NewSettingsCardReqBuilder().
		CardId(cardID).
		Body(larkcardkit.NewSettingsCardReqBodyBuilder().
			Settings(string(settings)).
			Sequence(sequence).
			Build()).
		Build()
	return p.withTransientRetry(ctx, "cardkit settings", func() error {
		return p.withFreshTenantAccessTokenRetry(ctx, "cardkit settings", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
			resp, err := client.Cardkit.V1.Card.Settings(ctx, req, options...)
			if err != nil {
				return fmt.Errorf("%s: cardkit settings: %w", p.tag(), err)
			}
			if !resp.Success() {
				return fmt.Errorf("%s: cardkit settings code=%d msg=%s seq=%d", p.tag(), resp.Code, resp.Msg, sequence)
			}
			return nil
		})
	})
}

// sendCardKitMessage attaches a CardKit card_id to a chat via IM API.
// Uses the {"type":"card","data":{"card_id":"..."}} content format which
// tells Feishu this IM message renders its content from the CardKit entity.
// Returns the IM message_id (for reactions, replies, etc.).
func (p *Platform) sendCardKitMessage(ctx context.Context, rc replyContext, cardID string) (string, error) {
	content := fmt.Sprintf(`{"type":"card","data":{"card_id":"%s"}}`, cardID)

	if p.shouldUseThreadOrReplyAPI(rc) {
		req := larkim.NewReplyMessageReqBuilder().
			MessageId(rc.messageID).
			Body(p.buildReplyMessageReqBody(rc, larkim.MsgTypeInteractive, content)).
			Build()
		var resp *larkim.ReplyMessageResp
		if err := p.withTransientRetry(ctx, "cardkit send (reply)", func() error {
			return p.withFreshTenantAccessTokenRetry(ctx, "cardkit send (reply)", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
				var err error
				resp, err = client.Im.Message.Reply(ctx, req, options...)
				if err != nil {
					return fmt.Errorf("%s: cardkit send (reply): %w", p.tag(), err)
				}
				if !resp.Success() {
					return fmt.Errorf("%s: cardkit send (reply) code=%d msg=%s", p.tag(), resp.Code, resp.Msg)
				}
				return nil
			})
		}); err != nil {
			return "", err
		}
		if resp.Data != nil && resp.Data.MessageId != nil {
			return *resp.Data.MessageId, nil
		}
		return "", fmt.Errorf("%s: cardkit send (reply): no message_id", p.tag())
	}

	req := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(larkim.ReceiveIdTypeChatId).
		Body(larkim.NewCreateMessageReqBodyBuilder().
			ReceiveId(rc.chatID).
			MsgType(larkim.MsgTypeInteractive).
			Content(content).
			Build()).
		Build()
	var resp *larkim.CreateMessageResp
	if err := p.withTransientRetry(ctx, "cardkit send", func() error {
		return p.withFreshTenantAccessTokenRetry(ctx, "cardkit send", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
			var err error
			resp, err = client.Im.Message.Create(ctx, req, options...)
			if err != nil {
				return fmt.Errorf("%s: cardkit send: %w", p.tag(), err)
			}
			if !resp.Success() {
				return fmt.Errorf("%s: cardkit send code=%d msg=%s", p.tag(), resp.Code, resp.Msg)
			}
			return nil
		})
	}); err != nil {
		return "", err
	}
	if resp.Data != nil && resp.Data.MessageId != nil {
		return *resp.Data.MessageId, nil
	}
	return "", fmt.Errorf("%s: cardkit send: no message_id", p.tag())
}

// FinalizePreview closes CardKit streaming_mode on the card referenced by
// previewHandle. Called by streamPreview.finish after the last content push.
// Non-streaming handles are ignored (legacy path needs no finalize).
func (p *Platform) FinalizePreview(ctx context.Context, previewHandle any) error {
	sh, ok := previewHandle.(*feishuStreamingHandle)
	if !ok {
		return nil // legacy handle, nothing to finalize
	}
	return p.setCardStreamingMode(ctx, sh.cardID, false, sh.nextSeq())
}

// buildStreamingCardJSON builds the initial card JSON with streaming_mode
// enabled and a single markdown element that will receive streamed text.
// The element_id must match streamingElementID so subsequent
// streamCardContent calls target the correct element.
func buildStreamingCardJSON(initialContent string) string {
	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{
			"wide_screen_mode": true,
			"streaming_mode":   true,
		},
		"body": map[string]any{
			"elements": []any{
				map[string]any{
					"tag":        "markdown",
					"content":    initialContent,
					"text_align": "left",
					"text_size":  "normal_v2",
					"element_id": streamingElementID,
				},
			},
		},
	}
	b, _ := json.Marshal(card)
	return string(b)
}
