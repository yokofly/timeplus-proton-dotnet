# Timeplus Proton from .NET — no driver required

There is no official .NET/C# driver for [Timeplus Proton](https://github.com/timeplus-io/proton)
(the shipped drivers are Java/JDBC, Go, Python, and an experimental ODBC one).
You don't need one: Proton speaks HTTP natively, so `HttpClient` and
`System.Text.Json` are enough — **zero NuGet dependencies**.

This repo is a ~100-line client plus a runnable demo that proves streaming
queries reach a .NET process incrementally, row by row, as data is ingested.

## Run it

```bash
docker compose up --exit-code-from app
```

Starts a Proton container plus a `dotnet/sdk:8.0` container and runs three
self-checking demos. Run just one with `dotnet run -- basic|random|mv`.

**`basic`** — `CREATE STREAM`, REST ingest, subscription, historical read.
Ingests three rows three seconds apart and asserts they arrive one at a time:

```
  [t+  2.5s] ingested  live-1
  [t+  2.5s] streamed  101 live-1
  [t+  5.5s] ingested  live-2
  [t+  5.5s] streamed  102 live-2
  [t+  8.5s] ingested  live-3
  [t+  8.5s] streamed  103 live-3

  historical read : 3 rows
  arrival spread  : 6.0s (buffered would be ~0s)
```

The `arrival spread` is the point. Had `HttpClient` buffered the response, all
three rows would have appeared together at the end.

**`random`** — `CREATE RANDOM STREAM` generates its own data at a set rate
(`SETTINGS eps = 20`), so you can exercise a consumer with no producer running
alongside it. Useful for load-testing your own code.

**`mv`** — a materialized view is a *continuous* query: it keeps running
server-side, streams its output, and stores it for historical reads. This one
tumbles a 2-second window over the random stream:

```
  2026-08-02 01:43:12.000  device_0  avg= 51.80  n=4
  2026-08-02 01:43:12.000  device_1  avg= 63.41  n=7
  2026-08-02 01:43:14.000  device_0  avg= 56.91  n=11
  2026-08-02 01:43:14.000  device_1  avg= 34.46  n=10
```

> **Disk note.** The compose file disables nativelog preallocation. By default
> Proton `fallocate()`s its log segments and a freshly started container reserves
> **~11 GB** — and because `fallocate` claims blocks past EOF, the file sizes
> still look tiny, so nothing in `ls` hints at it. Sensible for a real
> deployment, rough on throwaway test containers. Turning it off takes the same
> container to **~6 MB** with no behavioural change.

## Usage

```csharp
using var client = new ProtonClient("http://localhost:3218");

// write
await client.IngestAsync("my_stream", ["id", "msg"], [[1, "hello"]]);

// bounded historical read — note table(...)
var rows = await client.QueryAsync<Row>("SELECT id, msg FROM table(my_stream)");

// unbounded subscription — yields forever, cancel the token to stop
await foreach (var row in client.StreamAsync<Row>("SELECT id, msg FROM my_stream", ct))
    Console.WriteLine($"{row.id} {row.msg}");

record Row(int id, string msg);
```

Materialized views and random streams are just DDL, so they need no extra API:

```csharp
await client.ExecuteAsync("""
    CREATE RANDOM STREAM devices(
        device      string default 'device_' || to_string(rand() % 4),
        temperature float  default rand() % 1000 / 10)
    SETTINGS eps = 20
    """);

await client.ExecuteAsync("""
    CREATE MATERIALIZED VIEW device_stats AS
        SELECT window_start AS ts, device, round(avg(temperature), 2) AS avg_temp
        FROM tumble(devices, 2s)
        GROUP BY window_start, device
    """);

// subscribe to the view's output as it is computed
await foreach (var s in client.StreamAsync<Stat>("SELECT * FROM device_stats", ct))
    Console.WriteLine($"{s.ts} {s.device} {s.avg_temp}");
```

## Four things to know

**1. Streaming needs two specific settings.** `HttpCompletionOption.ResponseHeadersRead`
on the request, and `Timeout = Timeout.InfiniteTimeSpan` on the `HttpClient`.
Without the first, `HttpClient` buffers the whole body and an unbounded query
blocks forever. Without the second, the default 100s timeout kills a long-lived
subscription. Both are in [`ProtonClient.cs`](ProtonClient.cs) with comments.

**2. `SELECT * FROM my_stream` is a subscription, not a query.** It runs forever
and only returns rows that arrive *after* you connect. For a normal bounded read
use `SELECT * FROM table(my_stream)`. This trips up nearly everyone once.

**3. `window_start` is reserved — alias it in a materialized view.** A
`CREATE MATERIALIZED VIEW` that emits `window_start` as an output column fails
with `Column window_start is reserved, should not used in create query`. Write
`SELECT window_start AS ts, ... GROUP BY window_start` instead. Same for
`window_end`. See [`Demos.cs`](Demos.cs).

**4. Be careful with ClickHouse .NET clients.** ClickHouse.Driver, ClickHouse.Client,
and Octonica will partly work against the ClickHouse-compatible port 8123, but
Proton's type names are lowercase — `int32`, `string`, `datetime64(3)` instead of
`Int32`/`String`/`DateTime64`. Anything parsing type names out of RowBinary or the
native protocol will trip on type mapping. JSON formats sidestep it entirely,
at which point plain `HttpClient` is simpler anyway. *(Reasoned from Proton's type
naming and confirmed against its `/proton/v1/search` metadata — I did not run those
clients directly, so treat it as a caution, not a measurement.)*

## Endpoints

All on port **3218**:

| Endpoint | Purpose |
|---|---|
| `POST /` | Raw SQL. Add `?default_format=JSONEachRow` for NDJSON output. |
| `POST /proton/v1/ingest/streams/{name}` | JSON ingest — no SQL escaping to get wrong. |
| `POST /proton/v1/search` | Query via REST, returns `meta` + `data` + `statistics`. |
| `POST /proton/v1/ddl/streams` | Create streams via REST instead of DDL strings. |

Port **8463** is the native TCP protocol (what the JDBC/Go/Python drivers use);
**8123** is a ClickHouse-compatible HTTP port where queries are historical by
default. See [the Proton docs](https://docs.timeplus.com/proton) for the rest.

## Verified against

`timeplus/proton:latest` (v3.0.26, digest `sha256:8225284…`) on .NET 8, Ubuntu 24.04.

## License

MIT
