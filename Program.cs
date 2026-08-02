// Demo + self-check for ProtonClient. `docker compose up` runs this against a
// real Proton container and asserts that streaming delivery is truly incremental.

using System.Diagnostics;

var url = Environment.GetEnvironmentVariable("PROTON_URL") ?? "http://localhost:3218";
using var client = new ProtonClient(url);
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
var ct = cts.Token;

Console.WriteLine($"\n=== Timeplus Proton from .NET, no driver ({url}) ===\n");

// -- wait for the server ---------------------------------------------------
string version = "";
for (var i = 0; i < 60 && version.Length == 0; i++)
{
    // alias the column: JSONEachRow would otherwise key it "version()"
    try { version = (await client.QueryAsync<Ver>("SELECT version() AS version", ct))[0].version; }
    catch { await Task.Delay(1000, ct); }
}
if (version.Length == 0) { Console.WriteLine("server never came up"); return 1; }
Console.WriteLine($"connected to Proton {version}\n");

// -- schema ----------------------------------------------------------------
await client.ExecuteAsync("DROP STREAM IF EXISTS dotnet_demo", ct);
await client.ExecuteAsync("CREATE STREAM dotnet_demo (id int32, msg string)", ct);
Console.WriteLine("created stream dotnet_demo");

// -- subscribe BEFORE writing ----------------------------------------------
// A streaming SELECT only sees rows that arrive after it attaches, so start it
// first. Row arrival times are recorded to prove nothing is being buffered.
var arrivals = new List<double>();
var sw = Stopwatch.StartNew();

var subscription = Task.Run(async () =>
{
    await foreach (var row in client.StreamAsync<Row>("SELECT id, msg FROM dotnet_demo", ct))
    {
        arrivals.Add(sw.Elapsed.TotalSeconds);
        Console.WriteLine($"  [t+{sw.Elapsed.TotalSeconds,5:F1}s] streamed  {row.id} {row.msg}");
        if (arrivals.Count == 3) break;
    }
}, ct);

await Task.Delay(2500, ct); // give the subscription time to attach

// -- write -----------------------------------------------------------------
for (var i = 1; i <= 3; i++)
{
    await client.IngestAsync("dotnet_demo",
        ["id", "msg"], [[100 + i, $"live-{i}"]], ct);
    Console.WriteLine($"  [t+{sw.Elapsed.TotalSeconds,5:F1}s] ingested  live-{i}");
    await Task.Delay(3000, ct);
}

await subscription;

// -- historical read -------------------------------------------------------
// table(...) makes this a bounded query that actually terminates.
var all = await client.QueryAsync<Row>(
    "SELECT id, msg FROM table(dotnet_demo) ORDER BY id", ct);
Console.WriteLine($"\nhistorical read: {all.Count} rows -> " +
                  string.Join(", ", all.Select(r => $"{r.id}:{r.msg}")));

// -- assertions ------------------------------------------------------------
// If HttpClient were buffering, all three rows would land together at the end.
// A multi-second spread is the proof that delivery is genuinely incremental.
var spread = arrivals[^1] - arrivals[0];
var ok = arrivals.Count == 3 && all.Count == 3 && spread > 3.0;

Console.WriteLine($"arrival spread : {spread:F1}s (buffered would be ~0s)");
Console.WriteLine(ok ? "\n=== PASS ===\n" : "\n=== FAIL ===\n");

await client.ExecuteAsync("DROP STREAM IF EXISTS dotnet_demo", ct);
return ok ? 0 : 1;

record Row(int id, string msg);
record Ver(string version);
