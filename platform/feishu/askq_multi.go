// Multi-select AskUserQuestion card for Feishu.
//
// Renders q.Options as a <form> of <checker> rows plus a submit button.
// Submitting emits a card.action.trigger event with Name="askq_multi_submit"
// and FormValue map[checker_name]bool. The feishu adapter translates that
// back to "askq_multi:{qIdx}:1,3,5" text and forwards as a user message —
// engine.resolveAskQuestionAnswer parses the comma-separated indices into
// labels joined by ", ".
package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/chenhg5/cc-connect/core"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

const (
	askqMultiCheckerPrefix = "askq_opt_" // askq_opt_{qIdx}_{optIdx}
	askqMultiSubmitName    = "askq_multi_submit"
	askqMultiCancelName    = "askq_multi_cancel"
	askqMultiFormName      = "askq_multi_form"
)

// askqMultiCheckerName encodes qIdx+optIdx into the form checker's name.
func askqMultiCheckerName(qIdx, optIdx int) string {
	return fmt.Sprintf("%s%d_%d", askqMultiCheckerPrefix, qIdx, optIdx)
}

// parseAskqMultiCheckerName is the inverse — extracts optIdx from a checker
// name that belongs to the given qIdx. Returns (optIdx, ok).
func parseAskqMultiCheckerName(name string, qIdx int) (int, bool) {
	prefix := fmt.Sprintf("%s%d_", askqMultiCheckerPrefix, qIdx)
	if !strings.HasPrefix(name, prefix) {
		return 0, false
	}
	v, err := strconv.Atoi(strings.TrimPrefix(name, prefix))
	if err != nil {
		return 0, false
	}
	return v, true
}

// SendMultiSelectQuestion posts the checker+submit card for an AskUserQuestion
// multi-select. Implements core.MultiSelectQuestionSender.
func (p *Platform) SendMultiSelectQuestion(ctx context.Context, rctx any, q core.UserQuestion, qIdx, total int) error {
	if !p.useInteractiveCard {
		return core.ErrNotSupported
	}
	rc, ok := rctx.(replyContext)
	if !ok {
		return fmt.Errorf("%s: SendMultiSelectQuestion: invalid reply context type %T", p.tag(), rctx)
	}

	cardJSON := buildAskqMultiCardJSON(q, qIdx, total)
	if p.shouldUseThreadOrReplyAPI(rc) {
		req := larkim.NewReplyMessageReqBuilder().
			MessageId(rc.messageID).
			Body(p.buildReplyMessageReqBody(rc, larkim.MsgTypeInteractive, cardJSON)).
			Build()
		return p.withTransientRetry(ctx, "askq multi send (reply)", func() error {
			return p.withFreshTenantAccessTokenRetry(ctx, "askq multi send (reply)", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
				resp, err := client.Im.Message.Reply(ctx, req, options...)
				if err != nil {
					return fmt.Errorf("%s: askq multi send: %w", p.tag(), err)
				}
				if !resp.Success() {
					return fmt.Errorf("%s: askq multi send code=%d msg=%s", p.tag(), resp.Code, resp.Msg)
				}
				return nil
			})
		})
	}
	req := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(larkim.ReceiveIdTypeChatId).
		Body(larkim.NewCreateMessageReqBodyBuilder().
			ReceiveId(rc.chatID).
			MsgType(larkim.MsgTypeInteractive).
			Content(cardJSON).
			Build()).
		Build()
	return p.withTransientRetry(ctx, "askq multi send", func() error {
		return p.withFreshTenantAccessTokenRetry(ctx, "askq multi send", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
			resp, err := client.Im.Message.Create(ctx, req, options...)
			if err != nil {
				return fmt.Errorf("%s: askq multi send: %w", p.tag(), err)
			}
			if !resp.Success() {
				return fmt.Errorf("%s: askq multi send code=%d msg=%s", p.tag(), resp.Code, resp.Msg)
			}
			return nil
		})
	})
}

// buildAskqMultiCardJSON wraps checkers inside a <form> so submission
// delivers all selected values in one callback.
func buildAskqMultiCardJSON(q core.UserQuestion, qIdx, total int) string {
	title := "📋 AskUserQuestion"
	if total > 1 {
		title += fmt.Sprintf(" (%d/%d)", qIdx+1, total)
	}

	formElems := []map[string]any{
		{
			"tag":     "markdown",
			"content": "**" + q.Question + "**  ·  _多选_",
		},
	}
	for i, opt := range q.Options {
		desc := opt.Label
		if opt.Description != "" {
			desc += " — " + opt.Description
		}
		formElems = append(formElems, map[string]any{
			"tag":     "checker",
			"name":    askqMultiCheckerName(qIdx, i+1),
			"checked": false,
			"text": map[string]any{
				"tag":     "lark_md",
				"content": desc,
			},
		})
	}
	// Action row: submit + cancel
	formElems = append(formElems, map[string]any{
		"tag": "column_set",
		"columns": []map[string]any{
			{
				"tag":            "column",
				"width":          "auto",
				"vertical_align": "center",
				"elements": []map[string]any{
					{
						"tag":           "button",
						"text":          plainText("确定"),
						"type":          "primary",
						"name":          askqMultiSubmitName,
						"action_type":   "form_submit",
						"behaviors":     []map[string]any{{"type": "callback", "value": map[string]any{"askq_qidx": qIdx}}},
					},
				},
			},
			{
				"tag":            "column",
				"width":          "auto",
				"vertical_align": "center",
				"elements": []map[string]any{
					{
						"tag":         "button",
						"text":        plainText("取消"),
						"type":        "default",
						"name":        askqMultiCancelName,
						"action_type": "form_reset",
					},
				},
			},
		},
	})

	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{"wide_screen_mode": true},
		"header": map[string]any{
			"title":    map[string]any{"tag": "plain_text", "content": title},
			"template": "blue",
		},
		"body": map[string]any{
			"elements": []any{
				map[string]any{
					"tag":      "form",
					"name":     askqMultiFormName,
					"elements": formElems,
				},
			},
		},
	}
	b, _ := json.Marshal(card)
	return string(b)
}

// collectAskqMultiSelected reads the form submission's FormValue map and
// returns the selected option indices (1-based) in ascending order for
// the given qIdx.
func collectAskqMultiSelected(formValue map[string]any, qIdx int) []int {
	if formValue == nil {
		return nil
	}
	var selected []int
	for name, v := range formValue {
		optIdx, ok := parseAskqMultiCheckerName(name, qIdx)
		if !ok {
			continue
		}
		if b, ok := v.(bool); ok && b {
			selected = append(selected, optIdx)
		}
	}
	// Stable ascending order for reproducibility.
	for i := 1; i < len(selected); i++ {
		for j := i; j > 0 && selected[j-1] > selected[j]; j-- {
			selected[j-1], selected[j] = selected[j], selected[j-1]
		}
	}
	return selected
}
