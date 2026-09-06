# ace-datasource-clickhouse

Compile-time ClickHouse datasource module for [Ace](https://github.com/aceobservability/ace).

Ace keeps the datasource contract and registry in
`github.com/aceobservability/ace/backend/pkg/datasource`. This module implements
that `Client` (plus `QueryWithSignal` and connection test). Ace registers the
factory at `init` and injects its SSRF-safe HTTP client — this module does not
import Ace `internal/` packages and does not construct an unpolicy'd client.

## Contract

| Surface | Package |
| --- | --- |
| Query / result types | `github.com/aceobservability/ace/backend/pkg/datasource` |
| Registry type key | `clickhouse` (`Type`) |
| Factory | `New(url string, httpClient *http.Client, authConfig json.RawMessage)` |

`httpClient` is required. Ace passes `ssrf.DatasourceClient` wrapped with stored
datasource credentials. `authConfig` may include `"database"`; username/password
are applied by Ace's injected client, not this module.

## Tests

```
go test ./...
```

Query and connection tests speak to an `httptest` fixture. No live ClickHouse
is required.
