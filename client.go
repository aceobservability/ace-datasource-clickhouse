package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aceobservability/ace/backend/pkg/datasource"
)

// Type is the RegisterDatasource key Ace uses for this module.
const Type = "clickhouse"

const (
	clickHouseSignalLogs    = "logs"
	clickHouseSignalMetrics = "metrics"
	clickHouseSignalTraces  = "traces"
)

var clickHouseFormatPattern = regexp.MustCompile(`(?i)\bformat\b`)

// Client implements the Ace ClickHouse query datasource.
type Client struct {
	url        string
	httpClient *http.Client
	authConfig json.RawMessage
}

type clickHouseQueryResponse struct {
	Data []map[string]interface{} `json:"data"`
}

type clickHouseAuthConfig struct {
	Database string `json:"database"`
}

// New constructs a ClickHouse datasource client.
// httpClient is required so Ace can inject DatasourceClient (dial/redirect policy + auth).
func New(clickHouseURL string, httpClient *http.Client, authConfig json.RawMessage) (*Client, error) {
	if strings.TrimSpace(clickHouseURL) == "" {
		return nil, fmt.Errorf("datasource url is required")
	}
	if httpClient == nil {
		return nil, fmt.Errorf("http client is required")
	}

	return &Client{
		url:        clickHouseURL,
		httpClient: httpClient,
		authConfig: authConfig,
	}, nil
}

// HTTPClient returns the injected HTTP client. Ace SSRF tests inspect policy wiring.
func (c *Client) HTTPClient() *http.Client {
	return c.httpClient
}

func (c *Client) Query(ctx context.Context, query string, start, end time.Time, step time.Duration, limit int) (*datasource.QueryResult, error) {
	return c.QueryWithSignal(ctx, query, clickHouseSignalMetrics, start, end, step, limit)
}

func (c *Client) QueryWithSignal(ctx context.Context, query string, signal string, start, end time.Time, step time.Duration, limit int) (*datasource.QueryResult, error) {
	resolvedSignal := normalizeClickHouseSignal(signal)
	if resolvedSignal == "" {
		return nil, fmt.Errorf("invalid clickhouse signal %q, must be one of: logs, metrics, traces", signal)
	}

	rows, err := c.queryRows(ctx, query, start, end, step)
	if err != nil {
		return nil, err
	}

	switch resolvedSignal {
	case clickHouseSignalLogs:
		logs := NormaliseToLogs(rows)
		if limit > 0 && len(logs) > limit {
			logs = logs[:limit]
		}

		return &datasource.QueryResult{
			Status:     "success",
			ResultType: clickHouseSignalLogs,
			Data: &datasource.QueryData{
				ResultType: "streams",
				Logs:       logs,
			},
		}, nil
	case clickHouseSignalMetrics:
		metrics := NormaliseToMetrics(rows)
		if limit > 0 && len(metrics) > limit {
			metrics = metrics[:limit]
		}

		return &datasource.QueryResult{
			Status:     "success",
			ResultType: clickHouseSignalMetrics,
			Data: &datasource.QueryData{
				ResultType: "matrix",
				Result:     metrics,
			},
		}, nil
	case clickHouseSignalTraces:
		spans := NormaliseToTraces(rows)
		if limit > 0 && len(spans) > limit {
			spans = spans[:limit]
		}

		return &datasource.QueryResult{
			Status:     "success",
			ResultType: clickHouseSignalTraces,
			Data: &datasource.QueryData{
				ResultType: "traces",
				Traces:     spans,
			},
		}, nil
	default:
		return nil, fmt.Errorf("invalid clickhouse signal %q, must be one of: logs, metrics, traces", signal)
	}
}

func (c *Client) queryRows(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]map[string]interface{}, error) {
	trimmedQuery := strings.TrimSpace(query)
	if trimmedQuery == "" {
		return nil, fmt.Errorf("query is required")
	}

	queryWithRange := interpolateClickHouseTimeRange(trimmedQuery, start, end, step)
	body := ensureClickHouseJSONFormat(queryWithRange)

	targetURL, err := c.queryURL()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create clickhouse request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query clickhouse: %w", err)
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read clickhouse response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("authentication failed with status %d", resp.StatusCode)
		}

		message := strings.TrimSpace(string(payload))
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}

		return nil, fmt.Errorf("clickhouse query failed with status %d: %s", resp.StatusCode, message)
	}

	var decoded clickHouseQueryResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, fmt.Errorf("failed to parse clickhouse response: %w", err)
	}

	if decoded.Data == nil {
		return []map[string]interface{}{}, nil
	}

	return decoded.Data, nil
}

func (c *Client) queryURL() (string, error) {
	parsed, err := url.Parse(c.url)
	if err != nil {
		return "", fmt.Errorf("invalid datasource url: %w", err)
	}

	if strings.TrimSpace(parsed.Path) == "" {
		parsed.Path = "/"
	}

	values := parsed.Query()
	if database := parseClickHouseDatabase(c.authConfig); database != "" {
		values.Set("database", database)
	}
	parsed.RawQuery = values.Encode()

	return parsed.String(), nil
}

func parseClickHouseDatabase(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}

	var cfg clickHouseAuthConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return ""
	}

	return strings.TrimSpace(cfg.Database)
}

func normalizeClickHouseSignal(signal string) string {
	normalized := strings.ToLower(strings.TrimSpace(signal))
	if normalized == "" {
		return clickHouseSignalMetrics
	}

	switch normalized {
	case clickHouseSignalLogs, clickHouseSignalMetrics, clickHouseSignalTraces:
		return normalized
	default:
		return ""
	}
}

func interpolateClickHouseTimeRange(query string, start, end time.Time, step time.Duration) string {
	stepSeconds := int64(step / time.Second)
	if stepSeconds <= 0 {
		stepSeconds = 1
	}

	replacer := strings.NewReplacer(
		"{start}", strconv.FormatInt(start.Unix(), 10),
		"{end}", strconv.FormatInt(end.Unix(), 10),
		"{step}", strconv.FormatInt(stepSeconds, 10),
		"{start_ms}", strconv.FormatInt(start.UnixMilli(), 10),
		"{end_ms}", strconv.FormatInt(end.UnixMilli(), 10),
		"{start_ns}", strconv.FormatInt(start.UnixNano(), 10),
		"{end_ns}", strconv.FormatInt(end.UnixNano(), 10),
	)

	return replacer.Replace(query)
}

func ensureClickHouseJSONFormat(query string) string {
	trimmedQuery := strings.TrimSpace(query)
	if clickHouseFormatPattern.MatchString(trimmedQuery) {
		return trimmedQuery
	}

	return trimmedQuery + " FORMAT JSON"
}

var (
	_ datasource.Client            = (*Client)(nil)
	_ datasource.SignalQueryClient = (*Client)(nil)
)
