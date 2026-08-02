using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

/// <summary>
/// Minimal Timeplus Proton client over the HTTP interface. No NuGet packages.
/// Port 3218 serves both the SQL endpoint (POST /) and the REST API (/proton/v1/*).
/// </summary>
public sealed class ProtonClient : IDisposable
{
    private readonly HttpClient _http;
    private readonly string _baseUrl;

    public ProtonClient(string baseUrl = "http://localhost:3218")
    {
        _baseUrl = baseUrl.TrimEnd('/');

        // Infinite timeout is REQUIRED. A streaming SELECT never completes, so
        // HttpClient's default 100s timeout would kill it mid-flight. Lifetime
        // is controlled with a CancellationToken on each call instead.
        _http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
    }

    /// <summary>DDL, INSERT, anything with no result set worth reading.</summary>
    public async Task ExecuteAsync(string sql, CancellationToken ct = default)
    {
        using var resp = await _http.PostAsync(
            $"{_baseUrl}/", new StringContent(sql, Encoding.UTF8), ct);
        await ThrowIfErrorAsync(resp, ct);
    }

    /// <summary>
    /// Bounded query — reads to completion. Wrap the stream in table(...) for a
    /// historical read: "SELECT * FROM table(my_stream)". Passing an unbounded
    /// "SELECT * FROM my_stream" here will never return; use StreamAsync.
    /// </summary>
    public async Task<List<T>> QueryAsync<T>(string sql, CancellationToken ct = default)
    {
        var rows = new List<T>();
        await foreach (var row in StreamAsync<T>(sql, ct))
            rows.Add(row);
        return rows;
    }

    /// <summary>
    /// Streaming query. For "SELECT ... FROM my_stream" this yields each row at
    /// the moment it is produced and never completes on its own — cancel the
    /// token to stop. Server replies chunked as application/x-ndjson.
    /// </summary>
    public async IAsyncEnumerable<T> StreamAsync<T>(
        string sql, [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(
            HttpMethod.Post, $"{_baseUrl}/?default_format=JSONEachRow")
        {
            Content = new StringContent(sql, Encoding.UTF8),
        };

        // ResponseHeadersRead is load-bearing. The default (ResponseContentRead)
        // buffers the whole body before returning, so an unbounded query would
        // block here forever and rows would never surface.
        using var resp = await _http.SendAsync(
            req, HttpCompletionOption.ResponseHeadersRead, ct);
        await ThrowIfErrorAsync(resp, ct);

        await using var body = await resp.Content.ReadAsStreamAsync(ct);
        using var reader = new StreamReader(body);

        while (await reader.ReadLineAsync(ct) is { } line)
        {
            if (line.Length == 0) continue;
            yield return JsonSerializer.Deserialize<T>(line)!;
        }
    }

    /// <summary>
    /// REST ingest. Usually less fiddly than building an INSERT statement, and
    /// it takes plain JSON values — no SQL escaping to get wrong.
    /// </summary>
    public async Task IngestAsync(
        string stream, string[] columns, IEnumerable<object?[]> rows,
        CancellationToken ct = default)
    {
        var payload = JsonSerializer.Serialize(new { columns, data = rows });
        using var resp = await _http.PostAsync(
            $"{_baseUrl}/proton/v1/ingest/streams/{stream}",
            new StringContent(payload, Encoding.UTF8, "application/json"), ct);
        await ThrowIfErrorAsync(resp, ct);
    }

    private static async Task ThrowIfErrorAsync(HttpResponseMessage resp, CancellationToken ct)
    {
        if (resp.IsSuccessStatusCode) return;
        throw new ProtonException(
            (int)resp.StatusCode, await resp.Content.ReadAsStringAsync(ct));
    }

    public void Dispose() => _http.Dispose();
}

public sealed class ProtonException(int status, string body)
    : Exception($"Proton HTTP {status}: {body}")
{
    public int Status { get; } = status;
    public string Body { get; } = body;
}
