package cpa

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Account struct {
	ID               string         `json:"id"`
	FileName         string         `json:"file_name"`
	Provider         string         `json:"provider"`
	AccessToken      string         `json:"-"`
	RefreshToken     string         `json:"-"`
	IDToken          string         `json:"-"`
	ChatGPTAccountID string         `json:"chatgpt_account_id"`
	ChatGPTUserID    string         `json:"chatgpt_user_id"`
	Email            string         `json:"email"`
	Disabled         bool           `json:"disabled"`
	Priority         int            `json:"priority"`
	ExpiresAt        time.Time      `json:"expires_at"`
	RawJSON          map[string]any `json:"-"`
}

func ParseAccountJSON(filename string, raw []byte) (*Account, error) {
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("invalid json in %s: %w", filename, err)
	}

	provider, _ := doc["provider"].(string)
	if provider == "" {
		if _, ok := doc["chatgpt_account_id"]; ok {
			provider = "codex"
		} else if _, ok := doc["account_id"]; ok {
			provider = "codex"
		}
	}

	accessToken, _ := doc["access_token"].(string)
	refreshToken, _ := doc["refresh_token"].(string)
	idToken, _ := doc["id_token"].(string)

	disabled, _ := doc["disabled"].(bool)
	priority := 0
	if p, ok := doc["priority"].(float64); ok {
		priority = int(p)
	}

	var expiresAt time.Time
	if expStr, ok := doc["expired"].(string); ok {
		expiresAt, _ = time.Parse(time.RFC3339, expStr)
	} else if expStr, ok := doc["expires_at"].(string); ok {
		expiresAt, _ = time.Parse(time.RFC3339, expStr)
	}

	accountID, _ := doc["chatgpt_account_id"].(string)
	if accountID == "" {
		accountID, _ = doc["account_id"].(string)
	}

	email, _ := doc["email"].(string)
	userID, _ := doc["chatgpt_user_id"].(string)

	if idToken != "" {
		claims, err := decodeJWTClaims(idToken)
		if err == nil {
			if email == "" {
				if em, ok := claims["email"].(string); ok {
					email = em
				}
			}
			if accountID == "" {
				if act, ok := claims["https://api.openai.com/auth.chatgpt_account_id"].(string); ok {
					accountID = act
				} else if authMap, ok := claims["https://api.openai.com/auth"].(map[string]any); ok {
					if act, ok := authMap["chatgpt_account_id"].(string); ok {
						accountID = act
					}
					if uid, ok := authMap["chatgpt_user_id"].(string); ok && userID == "" {
						userID = uid
					}
				} else if act, ok := claims["chatgpt_account_id"].(string); ok {
					accountID = act
				}
			}
		}
	}

	id := filename
	if id == "" {
		id = accountID
	}

	return &Account{
		ID:               id,
		FileName:         filename,
		Provider:         provider,
		AccessToken:      accessToken,
		RefreshToken:     refreshToken,
		IDToken:          idToken,
		ChatGPTAccountID: accountID,
		ChatGPTUserID:    userID,
		Email:            email,
		Disabled:         disabled,
		Priority:         priority,
		ExpiresAt:        expiresAt,
		RawJSON:          doc,
	}, nil
}

func decodeJWTClaims(token string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil, errors.New("invalid jwt format")
	}
	seg := parts[1]
	if rem := len(seg) % 4; rem != 0 {
		seg += strings.Repeat("=", 4-rem)
	}
	data, err := base64.URLEncoding.DecodeString(seg)
	if err != nil {
		data, err = base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			return nil, err
		}
	}
	var claims map[string]any
	err = json.Unmarshal(data, &claims)
	return claims, err
}
