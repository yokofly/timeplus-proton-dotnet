using System.Diagnostics;

/// <summary>Three self-checking demos, each returning true on success.</summary>
public static class Demos
{
    // ------------------------------------------------------------- basic --
    // CREATE STREAM + REST ingest + subscription + historical read.
    // Proves rows reach .NET incrementally rather than in one buffered blob.
    public static async Task<bool> BasicAsync(ProtonClient c, CancellationToken ct)
    {
        Console.WriteLine("\n--- basic: stream, ingest, subscribe ---\n");

        await c.ExecuteAsync("DROP STREAM IF EXISTS dotnet_demo", ct);
        await c.ExecuteAsync("CREATE STREAM dotnet_demo (id int32, msg string)", ct);

        // Subscribe BEFORE writing: a streaming SELECT only sees rows that
        // arrive after it attaches.
        var arrivals = new List<double>();
        var sw = Stopwatch.StartNew();

        var sub = Task.Run(async () =>
        {
            await foreach (var r in c.StreamAsync<Row>("SELECT id, msg FROM dotnet_demo", ct))
            {
                arrivals.Add(sw.Elapsed.TotalSeconds);
                Console.WriteLine($"  [t+{sw.Elapsed.TotalSeconds,5:F1}s] streamed  {r.id} {r.msg}");
                if (arrivals.Count == 3) break;
            }
        }, ct);

        await Task.Delay(2500, ct);

        for (var i = 1; i <= 3; i++)
        {
            await c.IngestAsync("dotnet_demo", ["id", "msg"], [[100 + i, $"live-{i}"]], ct);
            Console.WriteLine($"  [t+{sw.Elapsed.TotalSeconds,5:F1}s] ingested  live-{i}");
            await Task.Delay(3000, ct);
        }

        await sub;

        // table(...) makes this a bounded query that actually terminates.
        var all = await c.QueryAsync<Row>(
            "SELECT id, msg FROM table(dotnet_demo) ORDER BY id", ct);

        // Buffered delivery would put all three arrivals at the same instant.
        var spread = arrivals[^1] - arrivals[0];
        Console.WriteLine($"\n  historical read : {all.Count} rows");
        Console.WriteLine($"  arrival spread  : {spread:F1}s (buffered would be ~0s)");

        await c.ExecuteAsync("DROP STREAM IF EXISTS dotnet_demo", ct);
        return arrivals.Count == 3 && all.Count == 3 && spread > 3.0;
    }

    // ------------------------------------------------------------ random --
    // A random stream generates its own data, so you can exercise a streaming
    // consumer with no producer to run alongside it. Handy for load tests.
    public static async Task<bool> RandomStreamAsync(ProtonClient c, CancellationToken ct)
    {
        Console.WriteLine("\n--- random: generated data, no producer needed ---\n");

        await c.ExecuteAsync("DROP STREAM IF EXISTS devices", ct);
        await c.ExecuteAsync("""
            CREATE RANDOM STREAM devices(
                device      string default 'device_' || to_string(rand() % 4),
                temperature float  default rand() % 1000 / 10)
            SETTINGS eps = 20
            """, ct);

        var seen = new List<Reading>();
        await foreach (var r in c.StreamAsync<Reading>(
            "SELECT device, temperature FROM devices", ct))
        {
            seen.Add(r);
            Console.WriteLine($"  {r.device}  {r.temperature,6:F1}");
            if (seen.Count == 8) break;
        }

        await c.ExecuteAsync("DROP STREAM IF EXISTS devices", ct);
        return seen.Count == 8;
    }

    // ---------------------------------------------------------------- mv --
    // A materialized view is a continuous query: it keeps running server-side
    // and both streams its output and stores it for historical reads.
    public static async Task<bool> MaterializedViewAsync(ProtonClient c, CancellationToken ct)
    {
        Console.WriteLine("\n--- mv: continuous tumbling-window aggregate ---\n");

        await c.ExecuteAsync("DROP VIEW IF EXISTS device_stats", ct);
        await c.ExecuteAsync("DROP STREAM IF EXISTS devices", ct);
        await c.ExecuteAsync("""
            CREATE RANDOM STREAM devices(
                device      string default 'device_' || to_string(rand() % 4),
                temperature float  default rand() % 1000 / 10)
            SETTINGS eps = 20
            """, ct);

        // window_start is a reserved name and cannot be an output column of a
        // CREATE MATERIALIZED VIEW -- alias it (here: AS ts) or the DDL fails
        // with "Column window_start is reserved".
        await c.ExecuteAsync("""
            CREATE MATERIALIZED VIEW device_stats AS
                SELECT window_start AS ts,
                       device,
                       round(avg(temperature), 2) AS avg_temp,
                       count() AS n
                FROM tumble(devices, 2s)
                GROUP BY window_start, device
            """, ct);

        var windows = new List<Stat>();
        await foreach (var s in c.StreamAsync<Stat>(
            "SELECT ts, device, avg_temp, n FROM device_stats", ct))
        {
            windows.Add(s);
            Console.WriteLine($"  {s.ts}  {s.device}  avg={s.avg_temp,6:F2}  n={s.n}");
            if (windows.Count == 8) break;
        }

        // The MV also persists its output, so it is queryable historically.
        var stored = await c.QueryAsync<Counted>(
            "SELECT count() AS rows_stored FROM table(device_stats)", ct);
        Console.WriteLine($"\n  rows stored by the view: {stored[0].rows_stored}");

        await c.ExecuteAsync("DROP VIEW IF EXISTS device_stats", ct);
        await c.ExecuteAsync("DROP STREAM IF EXISTS devices", ct);
        return windows.Count == 8 && stored[0].rows_stored > 0;
    }
}

public record Row(int id, string msg);
public record Reading(string device, double temperature);
public record Stat(string ts, string device, double avg_temp, long n);
public record Counted(long rows_stored);
public record Ver(string version);
