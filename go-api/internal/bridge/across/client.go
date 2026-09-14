package across

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is a thin HTTP client for the Across testnet API. No auth is sent
// by default -- live testing during Phase 7 planning confirmed the
// testnet API requires neither a Bearer key nor an integratorId, unlike
// the documented mainnet requirement. APIKey/IntegratorID remain wired as
// optional, harmless if the testnet API ever starts requiring them.
type Client struct {
	baseURL    string
	httpClient *http.Client
	APIKey     string
	IntegratorID string
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *Client) get(ctx context.Context, path string, query url.Values, out any) error {
	if c.IntegratorID != "" {
		query.Set("integratorId", c.IntegratorID)
	}
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("build request for %s: %w", path, err)
	}
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body from %s: %w", path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &APIError{Path: path, StatusCode: resp.StatusCode, Body: string(body)}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode response from %s: %w", path, err)
	}
	return nil
}

// APIError is a non-2xx response from the Across API, carrying the raw
// body so callers can pattern-match on known error shapes (e.g. status.go's
// DepositNotFoundException handling) without this package hardcoding every
// possible error type.
type APIError struct {
	Path       string
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("across api %s returned %d: %s", e.Path, e.StatusCode, e.Body)
}
