package clickhouse

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aceobservability/ace/backend/pkg/datasource"
)

var clickHouseTimestampColumns = []string{"timestamp", "time", "ts", "datetime", "date", "_time", "event_time"}
var clickHouseMessageColumns = []string{"message", "msg", "line", "log", "body", "text", "_msg"}
var clickHouseLevelColumns = []string{"level", "severity", "log_level", "severity_text", "lvl"}
var clickHouseMetricValueColumns = []string{"value", "val", "metric_value", "sum", "count", "avg", "min", "max"}
var clickHouseSpanIDColumns = []string{"span_id", "spanid", "spanId"}
var clickHouseParentSpanIDColumns = []string{"parent_span_id", "parentspanid", "parentSpanId"}
var clickHouseOperationColumns = []string{"operation_name", "operation", "span_name", "name"}
var clickHouseServiceColumns = []string{"service_name", "service", "serviceName"}
var clickHouseTraceStartColumns = []string{"start_time_unix_nano", "start_time_ns", "start_ns", "start_time", "timestamp", "time"}
var clickHouseTraceDurationColumns = []string{"duration_nano", "duration_ns", "duration", "duration_ms", "duration_us", "duration_seconds"}
var clickHouseTraceStatusColumns = []string{"status", "status_code", "otel_status_code", "otel.status_code"}

type clickHouseField struct {
	Key   string
	Value interface{}
}

type clickHouseMetricSeries struct {
	Metric map[string]string
	Values [][]interface{}
}

func NormaliseToLogs(rows []map[string]interface{}) []datasource.LogEntry {
	logs := make([]datasource.LogEntry, 0, len(rows))
	for _, row := range rows {
		timestampField, hasTimestamp := pickClickHouseField(row, clickHouseTimestampColumns)
		messageField, hasMessage := pickClickHouseField(row, clickHouseMessageColumns)

		if !hasTimestamp && !hasMessage && len(row) == 0 {
			continue
		}

		timestamp := ""
		if hasTimestamp {
			timestamp = formatClickHouseTimestamp(timestampField.Value)
		}

		line := ""
		if hasMessage {
			line = strings.TrimSpace(anyToString(messageField.Value))
		}
		if line == "" {
			line = marshalClickHouseRow(row)
		}

		excludedColumns := append(append([]string{}, clickHouseTimestampColumns...), clickHouseMessageColumns...)
		excludedColumns = append(excludedColumns, clickHouseLevelColumns...)
		labels := collectClickHouseLabels(row, excludedColumns)

		if levelField, ok := pickClickHouseField(row, clickHouseLevelColumns); ok {
			if level := strings.TrimSpace(anyToString(levelField.Value)); level != "" {
				labels["level"] = level
			}
		}

		logs = append(logs, datasource.LogEntry{
			Timestamp: timestamp,
			Line:      line,
			Labels:    labels,
			Level:     detectLogLevel(labels, line),
		})
	}

	return logs
}

func NormaliseToMetrics(rows []map[string]interface{}) []datasource.MetricResult {
	seriesBySignature := map[string]*clickHouseMetricSeries{}

	for _, row := range rows {
		timestampField, hasTimestamp := pickClickHouseField(row, clickHouseTimestampColumns)
		valueField, hasValue := pickClickHouseField(row, clickHouseMetricValueColumns)
		if !hasTimestamp || !hasValue {
			continue
		}

		timestampSeconds, ok := parseClickHouseTimestampSeconds(timestampField.Value)
		if !ok {
			continue
		}

		value, ok := parseClickHouseFloat(valueField.Value)
		if !ok {
			continue
		}

		excludedColumns := append(append([]string{}, clickHouseTimestampColumns...), clickHouseMetricValueColumns...)
		metric := collectClickHouseLabels(row, excludedColumns)
		if metricName := pickClickHouseMetricName(row); metricName != "" {
			metric["__name__"] = metricName
		}
		if _, ok := metric["__name__"]; !ok {
			metric["__name__"] = "value"
		}

		signature := clickHouseMetricSignature(metric)
		series, exists := seriesBySignature[signature]
		if !exists {
			series = &clickHouseMetricSeries{
				Metric: metric,
				Values: make([][]interface{}, 0, 32),
			}
			seriesBySignature[signature] = series
		}

		series.Values = append(series.Values, []interface{}{
			timestampSeconds,
			strconv.FormatFloat(value, 'f', -1, 64),
		})
	}

	if len(seriesBySignature) == 0 {
		return []datasource.MetricResult{}
	}

	signatures := make([]string, 0, len(seriesBySignature))
	for signature := range seriesBySignature {
		signatures = append(signatures, signature)
	}
	sort.Strings(signatures)

	results := make([]datasource.MetricResult, 0, len(signatures))
	for _, signature := range signatures {
		series := seriesBySignature[signature]
		sort.Slice(series.Values, func(i, j int) bool {
			return clickHouseValueTimestamp(series.Values[i]) < clickHouseValueTimestamp(series.Values[j])
		})

		results = append(results, datasource.MetricResult{
			Metric: series.Metric,
			Values: series.Values,
		})
	}

	return results
}

func NormaliseToTraces(rows []map[string]interface{}) []datasource.TraceSpan {
	spans := make([]datasource.TraceSpan, 0, len(rows))
	for _, row := range rows {
		spanIDField, ok := pickClickHouseField(row, clickHouseSpanIDColumns)
		if !ok {
			continue
		}

		spanID := strings.TrimSpace(anyToString(spanIDField.Value))
		if spanID == "" {
			continue
		}

		parentSpanID := ""
		if field, ok := pickClickHouseField(row, clickHouseParentSpanIDColumns); ok {
			parentSpanID = strings.TrimSpace(anyToString(field.Value))
		}

		operationName := ""
		if field, ok := pickClickHouseField(row, clickHouseOperationColumns); ok {
			operationName = strings.TrimSpace(anyToString(field.Value))
		}

		serviceName := ""
		if field, ok := pickClickHouseField(row, clickHouseServiceColumns); ok {
			serviceName = strings.TrimSpace(anyToString(field.Value))
		}

		startTimeUnixNano := int64(0)
		if field, ok := pickClickHouseField(row, clickHouseTraceStartColumns); ok {
			startTimeUnixNano = parseClickHouseTimestampNanos(field.Value, field.Key)
		}

		durationNano := int64(0)
		if field, ok := pickClickHouseField(row, clickHouseTraceDurationColumns); ok {
			durationNano = parseClickHouseDurationNanos(field.Value, field.Key)
		}

		tags := map[string]string{}
		if tagsField, ok := pickClickHouseField(row, []string{"tags", "attributes"}); ok {
			for key, value := range parseClickHouseTagMap(tagsField.Value) {
				tags[key] = value
			}
		}

		excludedColumns := append(append([]string{}, clickHouseSpanIDColumns...), clickHouseParentSpanIDColumns...)
		excludedColumns = append(excludedColumns, clickHouseOperationColumns...)
		excludedColumns = append(excludedColumns, clickHouseServiceColumns...)
		excludedColumns = append(excludedColumns, clickHouseTraceStartColumns...)
		excludedColumns = append(excludedColumns, clickHouseTraceDurationColumns...)
		excludedColumns = append(excludedColumns, clickHouseTraceStatusColumns...)
		excludedColumns = append(excludedColumns, "trace_id", "traceid", "tags", "attributes")
		for key, value := range collectClickHouseLabels(row, excludedColumns) {
			tags[key] = value
		}

		status := ""
		if field, ok := pickClickHouseField(row, clickHouseTraceStatusColumns); ok {
			status = strings.TrimSpace(anyToString(field.Value))
		}

		span := datasource.TraceSpan{
			SpanID:            spanID,
			ParentSpanID:      parentSpanID,
			OperationName:     operationName,
			ServiceName:       serviceName,
			StartTimeUnixNano: startTimeUnixNano,
			DurationNano:      max(durationNano, 0),
			Status:            status,
		}
		if len(tags) > 0 {
			span.Tags = tags
		}

		spans = append(spans, span)
	}

	return spans
}

func pickClickHouseMetricName(row map[string]interface{}) string {
	if field, ok := pickClickHouseField(row, []string{"__name__", "metric_name", "metric", "name", "series"}); ok {
		return strings.TrimSpace(anyToString(field.Value))
	}

	return ""
}

func pickClickHouseField(row map[string]interface{}, candidates []string) (clickHouseField, bool) {
	if len(row) == 0 {
		return clickHouseField{}, false
	}

	fieldsByNormalizedName := make(map[string]clickHouseField, len(row))
	for key, value := range row {
		normalized := normalizeClickHouseColumnName(key)
		if normalized == "" {
			continue
		}

		if _, exists := fieldsByNormalizedName[normalized]; !exists {
			fieldsByNormalizedName[normalized] = clickHouseField{Key: key, Value: value}
		}
	}

	for _, candidate := range candidates {
		if field, ok := fieldsByNormalizedName[normalizeClickHouseColumnName(candidate)]; ok {
			return field, true
		}
	}

	return clickHouseField{}, false
}

func collectClickHouseLabels(row map[string]interface{}, excludedColumns []string) map[string]string {
	excluded := make(map[string]struct{}, len(excludedColumns))
	for _, column := range excludedColumns {
		excluded[normalizeClickHouseColumnName(column)] = struct{}{}
	}

	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	labels := map[string]string{}
	for _, key := range keys {
		if _, skip := excluded[normalizeClickHouseColumnName(key)]; skip {
			continue
		}

		value := strings.TrimSpace(anyToString(row[key]))
		if value == "" {
			continue
		}

		labels[key] = value
	}

	return labels
}

func parseClickHouseTagMap(value interface{}) map[string]string {
	tags := map[string]string{}

	switch typed := value.(type) {
	case map[string]interface{}:
		for key, rawValue := range typed {
			trimmedKey := strings.TrimSpace(key)
			if trimmedKey == "" {
				continue
			}

			tags[trimmedKey] = anyToString(rawValue)
		}
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return tags
		}

		var decoded map[string]interface{}
		if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
			return tags
		}

		for key, rawValue := range decoded {
			trimmedKey := strings.TrimSpace(key)
			if trimmedKey == "" {
				continue
			}

			tags[trimmedKey] = anyToString(rawValue)
		}
	}

	return tags
}

func clickHouseMetricSignature(metric map[string]string) string {
	if len(metric) == 0 {
		return ""
	}

	keys := make([]string, 0, len(metric))
	for key := range metric {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+metric[key])
	}

	return strings.Join(parts, "|")
}

func clickHouseValueTimestamp(value []interface{}) float64 {
	if len(value) == 0 {
		return 0
	}

	timestamp, ok := parseClickHouseFloat(value[0])
	if !ok {
		return 0
	}

	return timestamp
}

func formatClickHouseTimestamp(value interface{}) string {
	if value == nil {
		return ""
	}

	if typed, ok := value.(time.Time); ok {
		return typed.UTC().Format(time.RFC3339Nano)
	}

	if typed, ok := value.(string); ok {
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return ""
		}

		if parsed, ok := parseClickHouseTimeString(trimmed); ok {
			return parsed.UTC().Format(time.RFC3339Nano)
		}

		if numeric, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return clickHouseSecondsToRFC3339(normalizeClickHouseEpochSeconds(numeric))
		}

		return trimmed
	}

	if seconds, ok := parseClickHouseTimestampSeconds(value); ok {
		return clickHouseSecondsToRFC3339(seconds)
	}

	return ""
}

func clickHouseSecondsToRFC3339(seconds float64) string {
	nanos := int64(math.Round(seconds * float64(time.Second)))
	return time.Unix(0, nanos).UTC().Format(time.RFC3339Nano)
}

func parseClickHouseTimestampSeconds(value interface{}) (float64, bool) {
	if value == nil {
		return 0, false
	}

	if typed, ok := value.(time.Time); ok {
		return float64(typed.UnixNano()) / float64(time.Second), true
	}

	if typed, ok := value.(string); ok {
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, false
		}

		if parsed, ok := parseClickHouseTimeString(trimmed); ok {
			return float64(parsed.UnixNano()) / float64(time.Second), true
		}

		numeric, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0, false
		}

		return normalizeClickHouseEpochSeconds(numeric), true
	}

	numeric, ok := anyToFloat64(value)
	if !ok {
		return 0, false
	}

	return normalizeClickHouseEpochSeconds(numeric), true
}

func parseClickHouseTimestampNanos(value interface{}, columnName string) int64 {
	if value == nil {
		return 0
	}

	if typed, ok := value.(time.Time); ok {
		return typed.UnixNano()
	}

	if typed, ok := value.(string); ok {
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0
		}

		if parsed, ok := parseClickHouseTimeString(trimmed); ok {
			return parsed.UnixNano()
		}

		numeric, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0
		}

		return normalizeClickHouseTimestampToNanos(numeric, columnName)
	}

	numeric, ok := anyToFloat64(value)
	if !ok {
		return 0
	}

	return normalizeClickHouseTimestampToNanos(numeric, columnName)
}

func parseClickHouseDurationNanos(value interface{}, columnName string) int64 {
	numeric, ok := anyToFloat64(value)
	if !ok {
		if typed, typedOK := value.(string); typedOK {
			parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
			if err != nil {
				return 0
			}
			numeric = parsed
			ok = true
		}
	}

	if !ok {
		return 0
	}

	normalizedName := normalizeClickHouseColumnName(columnName)
	switch {
	case strings.Contains(normalizedName, "nano") || strings.HasSuffix(normalizedName, "ns"):
		return int64(numeric)
	case strings.Contains(normalizedName, "micro") || strings.HasSuffix(normalizedName, "us"):
		return int64(numeric * 1_000)
	case strings.Contains(normalizedName, "milli") || strings.HasSuffix(normalizedName, "ms"):
		return int64(numeric * 1_000_000)
	case strings.Contains(normalizedName, "second") || strings.HasSuffix(normalizedName, "s"):
		return int64(numeric * float64(time.Second))
	default:
		return int64(numeric)
	}
}

func normalizeClickHouseEpochSeconds(value float64) float64 {
	absValue := math.Abs(value)
	switch {
	case absValue >= 1e18:
		return value / 1e9
	case absValue >= 1e15:
		return value / 1e6
	case absValue >= 1e12:
		return value / 1e3
	default:
		return value
	}
}

func normalizeClickHouseTimestampToNanos(value float64, columnName string) int64 {
	normalizedName := normalizeClickHouseColumnName(columnName)
	switch {
	case strings.Contains(normalizedName, "nano") || strings.HasSuffix(normalizedName, "ns"):
		return int64(value)
	case strings.Contains(normalizedName, "micro") || strings.HasSuffix(normalizedName, "us"):
		return int64(value * 1_000)
	case strings.Contains(normalizedName, "milli") || strings.HasSuffix(normalizedName, "ms"):
		return int64(value * 1_000_000)
	case strings.Contains(normalizedName, "second") || strings.HasSuffix(normalizedName, "sec"):
		return int64(value * float64(time.Second))
	}

	absValue := math.Abs(value)
	switch {
	case absValue >= 1e18:
		return int64(value)
	case absValue >= 1e15:
		return int64(value * 1_000)
	case absValue >= 1e12:
		return int64(value * 1_000_000)
	default:
		return int64(value * float64(time.Second))
	}
}

func parseClickHouseTimeString(value string) (time.Time, bool) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, layout := range layouts {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed, true
		}

		parsed, err = time.ParseInLocation(layout, value, time.UTC)
		if err == nil {
			return parsed, true
		}
	}

	return time.Time{}, false
}

func parseClickHouseFloat(value interface{}) (float64, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		return parsed, true
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, false
		}
		parsed, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func normalizeClickHouseColumnName(name string) string {
	trimmed := strings.ToLower(strings.TrimSpace(name))
	if trimmed == "" {
		return ""
	}

	b := strings.Builder{}
	b.Grow(len(trimmed))
	for _, char := range trimmed {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') {
			b.WriteRune(char)
		}
	}

	return b.String()
}

func marshalClickHouseRow(row map[string]interface{}) string {
	payload, err := json.Marshal(row)
	if err != nil {
		return ""
	}

	return string(payload)
}
