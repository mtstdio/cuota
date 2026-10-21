package cpa

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func createFakeJWT(claims map[string]any) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payloadBytes, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(payloadBytes)
	return header + "." + payload + ".sig"
}

func TestParseAccountJSONStandard(t *testing.T) {
	raw := []byte(`{
		"access_token": "token-123",
		"refresh_token": "ref-456",
		"chatgpt_account_id": "acct-openai-001",
		"email": "user@example.com",
		"disabled": false,
		"priority": 10
	}`)

	acct, err := ParseAccountJSON("codex-test.json", raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if acct.ID != "codex-test.json" {
		t.Errorf("expected ID 'codex-test.json', got '%s'", acct.ID)
	}
	if acct.AccessToken != "token-123" {
		t.Errorf("expected access token 'token-123', got '%s'", acct.AccessToken)
	}
	if acct.ChatGPTAccountID != "acct-openai-001" {
		t.Errorf("expected account ID 'acct-openai-001', got '%s'", acct.ChatGPTAccountID)
	}
	if acct.Email != "user@example.com" {
		t.Errorf("expected email 'user@example.com', got '%s'", acct.Email)
	}
	if acct.Priority != 10 {
		t.Errorf("expected priority 10, got %d", acct.Priority)
	}
}

func TestParseAccountJSONWithJWT(t *testing.T) {
	claims := map[string]any{
		"email": "jwtuser@example.com",
		"https://api.openai.com/auth.chatgpt_account_id": "jwt-act-999",
	}
	jwt := createFakeJWT(claims)

	raw := []byte(`{
		"access_token": "token-xyz",
		"id_token": "` + jwt + `"
	}`)

	acct, err := ParseAccountJSON("codex-jwt.json", raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if acct.ChatGPTAccountID != "jwt-act-999" {
		t.Errorf("expected extracted account ID 'jwt-act-999', got '%s'", acct.ChatGPTAccountID)
	}
	if acct.Email != "jwtuser@example.com" {
		t.Errorf("expected extracted email 'jwtuser@example.com', got '%s'", acct.Email)
	}
}
