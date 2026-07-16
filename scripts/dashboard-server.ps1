<#
Serves one static HTML file over plain HTTP - the container-side counterpart to
generate-local-stack-summary.ps1's -OutFile, so that script's generated dashboard can be opened as
http://localhost:<port> instead of file:// (see that script's own header comment on why: Chromium's
Private Network Access check blocks a file:// page's fetches to the emulators' health endpoints
outright, which is what its refresh (&#x21bb;) buttons rely on).

Re-reads -FilePath fresh on every request rather than caching it at startup, so re-running
generate-local-stack-summary.ps1 -OutFile <same path> while this keeps running is picked up on the
next browser reload with no restart needed.

Usage (host, standalone):
    dashboard-server.ps1 -FilePath C:\path\to\summary.html
Usage (containerized - see setup-podman-kafka.bat's own comment at its podman run for this exact
invocation):
    pwsh -NoProfile -File /app/dashboard-server.ps1 -FilePath /app/dashboard.html -Port 8098 -ListenHost +
#>
param(
    [Parameter(Mandatory)]
    [string]$FilePath,
    [int]$Port = 8098,
    # See registration\events-api.ps1's identical -ListenHost doc comment: "localhost" (the
    # default) is exempt from the URL ACL reservation HttpListener otherwise needs admin rights
    # for on Windows, so it's the right default for a plain host run. Inside a container, a
    # listener bound only to "localhost" never sees traffic arriving through a published port
    # (-p 8098:8098 forwards to the container's external interface, not its loopback) - the
    # containerized invocation above passes "+" (HttpListener's wildcard-all-interfaces syntax)
    # to work around that; the Windows ACL restriction this default avoids doesn't apply inside a
    # Linux container anyway.
    [string]$ListenHost = 'localhost'
)

$listener = [System.Net.HttpListener]::new()
$listener.Prefixes.Add("http://${ListenHost}:$Port/")
$listener.Start()
Write-Host "Serving $FilePath at http://localhost:$Port/ (Ctrl+C to stop)"

try {
    while ($listener.IsListening) {
        try {
            $context = $listener.GetContext()
        } catch {
            # $listener.Stop() (Ctrl+C below, or a container being stopped) aborts a blocked
            # GetContext() call exactly like this - not a real error, just the shutdown signal.
            break
        }
        try {
            $bytes = [System.IO.File]::ReadAllBytes($FilePath)
            $context.Response.ContentType = 'text/html; charset=utf-8'
            $context.Response.ContentLength64 = $bytes.Length
            $context.Response.OutputStream.Write($bytes, 0, $bytes.Length)
        } catch {
            # Most likely $FilePath doesn't exist yet/right now (e.g. generate-local-stack-
            # summary.ps1 hasn't written it for the first time yet) - report it as a real error to
            # the browser rather than silently hanging or closing the connection.
            $context.Response.StatusCode = 500
        } finally {
            $context.Response.OutputStream.Close()
        }
    }
} finally {
    $listener.Stop()
}
