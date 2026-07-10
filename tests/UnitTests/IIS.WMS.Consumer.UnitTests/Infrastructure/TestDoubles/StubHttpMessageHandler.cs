namespace IIS.WMS.Consumer.UnitTests.Infrastructure.TestDoubles;

/// <summary>Records every request it receives and answers with whatever <see cref="Respond"/> returns for it, or a canned 200 OK by default.</summary>
public sealed class StubHttpMessageHandler : HttpMessageHandler
{
    public List<HttpRequestMessage> Requests { get; } = [];

    public Func<HttpRequestMessage, HttpResponseMessage> Respond { get; set; } =
        _ => new HttpResponseMessage(System.Net.HttpStatusCode.OK);

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Requests.Add(request);
        return Task.FromResult(Respond(request));
    }
}
