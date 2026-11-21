package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"autoclaw2api/internal/protocol"
)

func loadToken() string {
	entries, _ := filepath.Glob("/opt/autoclaw2api/auths/autoclawglobal-*.json")
	raw, _ := os.ReadFile(entries[0])
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	t, _ := m["accessToken"].(string)
	return t
}

func balance(client *http.Client, token string) float64 {
	url := protocol.DefaultBaseURLGlobal + protocol.PathWallets
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	protocol.AttachUserHeaders(req, token)
	resp, err := client.Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var env struct {
		Data struct {
			TotalBalance any `json:"total_balance"`
		} `json:"data"`
	}
	_ = json.Unmarshal(raw, &env)
	if f, ok := env.Data.TotalBalance.(float64); ok {
		return f
	}
	return -1
}

func chat(client *http.Client, token, model, prompt string) (int, int, int) {
	body, _ := json.Marshal(map[string]any{
		"model": model,
		"messages": []map[string]any{
			{"role": "system", "content": "You are a personal assistant running inside OpenClaw.\n\n## Tooling\nAvailable tools are policy-filtered. Names are case-sensitive; call exactly as listed.\n"},
			{"role": "user", "content": prompt},
		},
		"max_tokens": 10000,
	})
	prepared := protocol.PrepareOutboundChatBody(body, model)
	url := protocol.DefaultBaseURLGlobal + protocol.PathChat
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(prepared))
	protocol.AttachChatHeaders(req, token, model)
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, 0
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 300000))
	var out struct {
		Usage struct {
			Prompt     int `json:"prompt_tokens"`
			Completion int `json:"completion_tokens"`
			Total      int `json:"total_tokens"`
			Details    struct {
				Cached int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	_ = json.Unmarshal(raw, &out)
	return out.Usage.Prompt, out.Usage.Completion, out.Usage.Details.Cached
}

func run(round int, model string, size int) {
	token := loadToken()
	client := &http.Client{Timeout: 180 * time.Second}
	prompt := strings.Repeat("请只回复ok。", size/6)
	b0 := balance(client, token)
	tp, tc, cch := 0, 0, 0
	for i := 0; i < round; i++ {
		p, c, k := chat(client, token, model, prompt)
		tp += p
		tc += c
		cch += k
		time.Sleep(600 * time.Millisecond)
	}
	time.Sleep(12 * time.Second)
	b1 := balance(client, token)
	fmt.Printf("%-20s x%-2d | tokens p=%d c=%d cached=%d total=%d | Δ=%.0f credit | per1k=%.2f\n",
		model, round, tp, tc, cch, tp+tc, b0-b1, (b0-b1)/float64(tp+tc)*1000)
}

func main() {
	run(5, "zai_auto", 3000)
	run(5, "zaicoding_glm-5.3", 3000)
	run(5, "zai_glm-5.3-flash", 3000)
}
