// Runs the demos in Demos.cs against a live Proton instance.
//
//   docker compose up              -- all three
//   dotnet run -- basic|random|mv  -- just one

var url = Environment.GetEnvironmentVariable("PROTON_URL") ?? "http://localhost:3218";
var which = args.Length > 0 ? args[0].ToLowerInvariant() : "all";

using var client = new ProtonClient(url);
using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
var ct = cts.Token;

Console.WriteLine($"\n=== Timeplus Proton from .NET, no driver ({url}) ===");

// -- wait for the server ---------------------------------------------------
string version = "";
for (var i = 0; i < 60 && version.Length == 0; i++)
{
    // alias the column: JSONEachRow would otherwise key it "version()"
    try { version = (await client.QueryAsync<Ver>("SELECT version() AS version", ct))[0].version; }
    catch { await Task.Delay(1000, ct); }
}
if (version.Length == 0) { Console.WriteLine("server never came up"); return 1; }
Console.WriteLine($"connected to Proton {version}");

// -- run --------------------------------------------------------------------
var results = new List<(string Name, bool Ok)>();

if (which is "all" or "basic")
    results.Add(("basic", await Demos.BasicAsync(client, ct)));
if (which is "all" or "random")
    results.Add(("random", await Demos.RandomStreamAsync(client, ct)));
if (which is "all" or "mv")
    results.Add(("mv", await Demos.MaterializedViewAsync(client, ct)));

if (results.Count == 0)
{
    Console.WriteLine($"unknown demo '{which}' -- expected: basic, random, mv, all");
    return 2;
}

// -- summary ----------------------------------------------------------------
Console.WriteLine("\n" + new string('-', 44));
foreach (var (name, ok) in results)
    Console.WriteLine($"  {(ok ? "PASS" : "FAIL")}  {name}");

var allOk = results.All(r => r.Ok);
Console.WriteLine(allOk ? "\n=== PASS ===\n" : "\n=== FAIL ===\n");
return allOk ? 0 : 1;
