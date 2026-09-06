package clickhouse

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

func anyToString(value interface{}) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return fmt.Sprint(typed)
	}
}

func anyToFloat64(value interface{}) (float64, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case float64:
		return typed, true
	case int64:
		return float64(typed), true
	case int:
		return float64(typed), true
	case json.Number:
		floatVal, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		return floatVal, true
	case string:
		if typed == "" {
			return 0, false
		}
		floatVal, err := strconv.ParseFloat(typed, 64)
		if err != nil {
			return 0, false
		}
		return floatVal, true
	}

	return 0, false
}

func detectLogLevel(labels map[string]string, line string) string {
	for _, key := range []string{"level", "lvl", "severity", "severity_text"} {
		if level, ok := labels[key]; ok {
			if normalized := normalizeLogLevel(level); normalized != "" {
				return normalized
			}
		}
	}

	if extracted := extractStructuredLogLevel(line); extracted != "" {
		return extracted
	}

	lower := strings.ToLower(line)
	switch {
	case strings.Contains(lower, "error") || strings.Contains(lower, "err="):
		return "error"
	case strings.Contains(lower, "warn"):
		return "warning"
	case strings.Contains(lower, "info"):
		return "info"
	case strings.Contains(lower, "debug"):
		return "debug"
	default:
		return ""
	}
}

var structuredLevelPattern = regexp.MustCompile(`(?i)(?:^|[\s>\[(,])(?:level|lvl|severity|severity_text)=(?:"|')?(trace|debug|info|warn|warning|error|fatal|panic|critical)(?:\d+)?(?:"|')?(?:$|[\s,\])])`)

func extractStructuredLogLevel(line string) string {
	match := structuredLevelPattern.FindStringSubmatch(line)
	if len(match) < 2 {
		return ""
	}

	return normalizeLogLevel(match[1])
}

func normalizeLogLevel(level string) string {
	normalized := strings.ToLower(strings.TrimSpace(strings.Trim(level, `"'`)))
	if normalized == "" {
		return ""
	}

	switch {
	case strings.HasPrefix(normalized, "trace"):
		return "debug"
	case strings.HasPrefix(normalized, "debug") || normalized == "dbg":
		return "debug"
	case strings.HasPrefix(normalized, "info") || normalized == "information" || normalized == "inf":
		return "info"
	case strings.HasPrefix(normalized, "warn") || normalized == "wrn":
		return "warning"
	case strings.HasPrefix(normalized, "error") || normalized == "err":
		return "error"
	case strings.HasPrefix(normalized, "fatal") || strings.HasPrefix(normalized, "panic") || strings.HasPrefix(normalized, "critical") || normalized == "crit":
		return "error"
	case normalized == "unspecified" || normalized == "unknown" || normalized == "default":
		return ""
	default:
		return ""
	}
}
