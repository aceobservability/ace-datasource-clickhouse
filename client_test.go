package clickhouse

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNew_requiresHTTPClient(t *testing.T) {
	t.Parallel()

	client, err := New("http://localhost:8123", nil, nil)
	if err == nil {
		t.Fatal("expected error for nil http client")
	}
	if client != nil {
		t.Fatal("expected nil client when http client is missing")
	}
	if !strings.Contains(err.Error(), "http client is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNew_requiresURL(t *testing.T) {
	t.Parallel()

	client, err := New("  ", http.DefaultClient, nil)
	if err == nil {
		t.Fatal("expected error for empty url")
	}
	if client != nil {
		t.Fatal("expected nil client when url is missing")
	}
	if !strings.Contains(err.Error(), "datasource url is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormaliseToLogs(t *testing.T) {
	t.Parallel()

	rows := []map[string]interface{}{
		{
			"timestamp": "2026-02-18T10:00:00Z",
			"message":   "request failed",
			"severity":  "ERROR",
			"service":   "api",
		},
	}

	logs := NormaliseToLogs(rows)
	if len(logs) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(logs))
	}

	entry := logs[0]
	if entry.Timestamp != "2026-02-18T10:00:00Z" {
		t.Fatalf("expected timestamp 2026-02-18T10:00:00Z, got %q", entry.Timestamp)
	}
	if entry.Line != "request failed" {
		t.Fatalf("expected line request failed, got %q", entry.Line)
	}
	if entry.Level != "error" {
		t.Fatalf("expected level error, got %q", entry.Level)
	}
	if entry.Labels["service"] != "api" {
		t.Fatalf("expected service label api, got %q", entry.Labels["service"])
	}
}

func TestNormaliseToMetrics(t *testing.T) {
	t.Parallel()

	rows := []map[string]interface{}{
		{"timestamp": 1700000000, "value": 2.5, "host": "a", "metric_name": "cpu_usage"},
		{"timestamp": 1700000060, "value": 2.8, "host": "a", "metric_name": "cpu_usage"},
		{"timestamp": 1700000000, "value": 3.1, "host": "b", "metric_name": "cpu_usage"},
	}

	metrics := NormaliseToMetrics(rows)
	if len(metrics) != 2 {
		t.Fatalf("expected 2 metric series, got %d", len(metrics))
	}

	seriesByHost := map[string]struct {
		Metric map[string]string
		Values [][]interface{}
	}{}
	for _, series := range metrics {
		seriesByHost[series.Metric["host"]] = struct {
			Metric map[string]string
			Values [][]interface{}
		}{Metric: series.Metric, Values: series.Values}
	}

	seriesA, ok := seriesByHost["a"]
	if !ok {
		t.Fatalf("expected host=a series to exist")
	}
	if len(seriesA.Values) != 2 {
		t.Fatalf("expected host=a to have 2 values, got %d", len(seriesA.Values))
	}
	if seriesA.Metric["__name__"] != "cpu_usage" {
		t.Fatalf("expected metric name cpu_usage, got %q", seriesA.Metric["__name__"])
	}

	firstTimestamp, ok := parseClickHouseFloat(seriesA.Values[0][0])
	if !ok || firstTimestamp != 1700000000 {
		t.Fatalf("expected first timestamp 1700000000, got %v", seriesA.Values[0][0])
	}
}

func TestNormaliseToTraces(t *testing.T) {
	t.Parallel()

	rows := []map[string]interface{}{
		{
			"span_id":              "span-1",
			"parent_span_id":       "root",
			"operation_name":       "GET /health",
			"service_name":         "api",
			"start_time_unix_nano": int64(1700000000000000000),
			"duration_ns":          int64(5000000),
			"status_code":          "ERROR",
			"attributes": map[string]interface{}{
				"http.method": "GET",
			},
		},
		{
			"operation_name": "missing span id",
		},
	}

	spans := NormaliseToTraces(rows)
	if len(spans) != 1 {
		t.Fatalf("expected 1 trace span, got %d", len(spans))
	}

	span := spans[0]
	if span.SpanID != "span-1" {
		t.Fatalf("expected span id span-1, got %q", span.SpanID)
	}
	if span.ParentSpanID != "root" {
		t.Fatalf("expected parent span id root, got %q", span.ParentSpanID)
	}
	if span.ServiceName != "api" {
		t.Fatalf("expected service api, got %q", span.ServiceName)
	}
	if span.OperationName != "GET /health" {
		t.Fatalf("expected operation GET /health, got %q", span.OperationName)
	}
	if span.StartTimeUnixNano != 1700000000000000000 {
		t.Fatalf("expected start_time_unix_nano 1700000000000000000, got %d", span.StartTimeUnixNano)
	}
	if span.DurationNano != 5000000 {
		t.Fatalf("expected duration 5000000, got %d", span.DurationNano)
	}
	if span.Status != "ERROR" {
		t.Fatalf("expected status ERROR, got %q", span.Status)
	}
	if span.Tags["http.method"] != "GET" {
		t.Fatalf("expected tag http.method=GET, got %q", span.Tags["http.method"])
	}
}

func TestQueryWithSignal_usesDatabaseAndJSONFormat(t *testing.T) {
	t.Parallel()

	start := time.Unix(1700000000, 0)
	end := start.Add(5 * time.Minute)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST method, got %s", r.Method)
		}

		if r.URL.Query().Get("database") != "analytics" {
			t.Fatalf("expected database query param analytics, got %q", r.URL.Query().Get("database"))
		}

		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}

		body := string(payload)
		if !strings.Contains(body, "FORMAT JSON") {
			t.Fatalf("expected request body to contain FORMAT JSON, got %q", body)
		}
		if !strings.Contains(body, "1700000000") {
			t.Fatalf("expected placeholder substitution with start timestamp, got %q", body)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"timestamp":"2026-02-18T10:00:00Z","message":"ok","level":"info"}]}`))
	}))
	t.Cleanup(server.Close)

	client, err := New(server.URL, server.Client(), json.RawMessage(`{"database":"analytics"}`))
	if err != nil {
		t.Fatalf("failed to create clickhouse client: %v", err)
	}

	result, err := client.QueryWithSignal(
		context.Background(),
		"SELECT {start} AS start",
		"logs",
		start,
		end,
		15*time.Second,
		0,
	)
	if err != nil {
		t.Fatalf("unexpected query error: %v", err)
	}

	if result.ResultType != "logs" {
		t.Fatalf("expected result type logs, got %q", result.ResultType)
	}
	if result.Data == nil || len(result.Data.Logs) != 1 {
		t.Fatalf("expected 1 log result, got %+v", result.Data)
	}
}

func TestQueryAndTestConnection_againstFixtureHTTP(t *testing.T) {
	t.Parallel()

	var sawQuery, sawPing bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost:
			sawQuery = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"timestamp":1700000000,"value":1,"host":"a"}]}`))
		case r.URL.Path == "/ping":
			sawPing = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Ok."))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	client, err := New(srv.URL, srv.Client(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Unix(1700000000, 0).Add(-time.Hour)
	end := time.Unix(1700000000, 0)
	result, err := client.Query(ctx, "SELECT 1", start, end, time.Minute, 0)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("Query status=%q error=%q", result.Status, result.Error)
	}
	if result.ResultType != "metrics" {
		t.Fatalf("ResultType=%q, want metrics", result.ResultType)
	}
	if result.Data == nil || len(result.Data.Result) != 1 {
		t.Fatalf("expected 1 series, got %+v", result.Data)
	}
	if !sawQuery {
		t.Fatal("expected fixture to receive query POST")
	}

	if err := client.TestConnection(ctx); err != nil {
		t.Fatalf("TestConnection: %v", err)
	}
	if !sawPing {
		t.Fatal("expected TestConnection to hit /ping")
	}
}

func TestTestConnection_usesPingThenQueryFallback(t *testing.T) {
	t.Parallel()

	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path+"?"+r.URL.RawQuery)
		if r.URL.Path == "/ping" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.RawQuery == "query=SELECT 1" || strings.Contains(r.URL.RawQuery, "SELECT") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("1"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	client, err := New(srv.URL, srv.Client(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.TestConnection(ctx); err != nil {
		t.Fatalf("TestConnection: %v", err)
	}
	if len(paths) < 2 || paths[0] != "/ping?" {
		t.Fatalf("paths=%v, want /ping then query", paths)
	}
}
