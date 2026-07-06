<#
.SYNOPSIS
    Exports the list of Microsoft Fabric / Power BI workspaces in the same shape as the
    Admin Portal "Export" option, with these columns:

        ID, Name, Description, Type, State, Capacity name, Capacity SKU Tier

.DESCRIPTION
    Pulls the workspace list from the newer Fabric Admin REST API (v1/admin/workspaces) and
    resolves each workspace's capacity name + raw SKU (e.g. F64, P1) from the Power BI admin
    capacities API (the tenant-wide capacity list, joined on capacityId).

    Note: the Fabric admin workspaces API does not expose workspace Description (neither the
    list nor the per-workspace Get), so the Description column is written but left blank. The
    other six columns are fully populated.

    Features:
      * Streams results to CSV batch-by-batch (each API page is appended as it is read),
        so a long-running export against a large tenant never holds everything in memory
        and you get partial output even if it stops early.
      * Live progress reporting (Write-Progress + console lines).
      * 429 (throttling) handling that honours the Retry-After header.
      * Automatic retries for 429 / 5xx / transient network errors with exponential
        backoff + jitter.
      * Works on Windows PowerShell 5.1 and PowerShell 7+.

.PREREQUISITES
    * Fabric / Power BI tenant admin rights (Tenant.Read.All).
    * The script installs the MicrosoftPowerBIMgmt module if needed, then prompts you to
      choose an identity - interactive user sign-in (browser account picker) or a service
      principal. If you're already signed in, it asks whether to reuse that account or switch.
      For unattended runs, bypass the prompt by passing -AccessToken, or all three of
      -TenantId / -ServicePrincipalId / -ServicePrincipalSecret.

.PARAMETER OutputPath
    Path to the CSV file to write. Defaults to .\FabricWorkspaces_<timestamp>.csv

.PARAMETER BatchSize
    How many workspaces to buffer before flushing them to the CSV (one "batch"). The Fabric
    admin API controls its own page size via continuation tokens, so this just governs how
    often results are written to disk and progress is reported. Default 5000.

.PARAMETER AccessToken
    Optional. A bearer token string (with or without the "Bearer " prefix). If omitted the
    script calls Get-PowerBIAccessToken, so run Connect-PowerBIServiceAccount first.

.PARAMETER MaxRetries
    Max retry attempts per request before giving up. Default 6.

.PARAMETER TenantId
.PARAMETER ServicePrincipalId
.PARAMETER ServicePrincipalSecret
    Optional. Supply all three to sign in non-interactively with a service principal
    (app registration) instead of the interactive browser prompt.

.PARAMETER ModuleScope
    Scope used if the MicrosoftPowerBIMgmt module has to be installed. Default CurrentUser.

.PARAMETER SkipModuleSetup
    Skip the auto install / import / sign-in step (use when you pass -AccessToken, or when
    you have already connected in the current session).

.EXAMPLE
    # Self-contained: installs the module if needed, prompts you to pick the sign-in
    # identity, then exports.
    .\Export-FabricWorkspaces.ps1 -OutputPath .\workspaces.csv

.EXAMPLE
    # Unattended with a service principal.
    .\Export-FabricWorkspaces.ps1 -TenantId $tid -ServicePrincipalId $appId -ServicePrincipalSecret $secret

.EXAMPLE
    .\Export-FabricWorkspaces.ps1 -AccessToken $myToken -BatchSize 1000
#>

[CmdletBinding()]
param(
    [string] $OutputPath = ".\FabricWorkspaces_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv",
    [ValidateRange(1, 5000)]
    [int]    $BatchSize   = 5000,
    [string] $AccessToken,
    [ValidateRange(0, 20)]
    [int]    $MaxRetries  = 6,

    # --- Auth / bootstrap ---
    [string] $TenantId,
    [string] $ServicePrincipalId,
    [string] $ServicePrincipalSecret,
    [ValidateSet('CurrentUser', 'AllUsers')]
    [string] $ModuleScope = 'CurrentUser',
    [switch] $SkipModuleSetup,

    # Newer Fabric admin API surface (workspace list).
    [string] $ApiRoot        = 'https://api.fabric.microsoft.com/v1',

    # Power BI admin API - tenant-wide capacity list (capacity name + SKU).
    [string] $PowerBiApiRoot = 'https://api.powerbi.com/v1.0/myorg'
)

$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# Best-effort decode of the signed-in identity (UPN / app id) from a bearer
# token, so we can show the user which account they are connected as.
# ---------------------------------------------------------------------------
function Get-TokenIdentity {
    param([string]$Bearer)
    try {
        $jwt   = ($Bearer -replace '^(?i)bearer\s+', '')
        $parts = $jwt.Split('.')
        if ($parts.Count -lt 2) { return $null }
        $payload = $parts[1].Replace('-', '+').Replace('_', '/')
        switch ($payload.Length % 4) { 2 { $payload += '==' } 3 { $payload += '=' } }
        $claims = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($payload)) | ConvertFrom-Json
        foreach ($c in @('upn', 'preferred_username', 'unique_name', 'email', 'appid')) {
            if ($claims.$c) { return [string]$claims.$c }
        }
    } catch { }
    return $null
}

# ---------------------------------------------------------------------------
# Sign in with a service principal (app registration).
# ---------------------------------------------------------------------------
function Connect-ServicePrincipalIdentity {
    param(
        [Parameter(Mandatory)] [string] $TenantId,
        [Parameter(Mandatory)] [string] $AppId,
        [Parameter(Mandatory)] [System.Security.SecureString] $Secret
    )
    $cred = New-Object System.Management.Automation.PSCredential($AppId, $Secret)
    Connect-PowerBIServiceAccount -ServicePrincipal -Credential $cred -Tenant $TenantId | Out-Null
}

# ---------------------------------------------------------------------------
# Ensure the MicrosoftPowerBIMgmt module is installed, then sign in - prompting
# interactively for which identity to use unless a service principal (or a raw
# -AccessToken) was supplied on the command line.
# ---------------------------------------------------------------------------
function Initialize-PowerBIConnection {
    param(
        [string] $Scope,
        [string] $TenantId,
        [string] $ServicePrincipalId,
        [string] $ServicePrincipalSecret
    )

    # PowerShell Gallery needs TLS 1.2 (matters on Windows PowerShell 5.1).
    try { [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12 } catch { }

    # Install the module if it isn't already available.
    if (-not (Get-Module -ListAvailable -Name MicrosoftPowerBIMgmt)) {
        Write-Host "Installing MicrosoftPowerBIMgmt module (scope $Scope)..." -ForegroundColor Cyan
        if (-not (Get-PackageProvider -Name NuGet -ErrorAction SilentlyContinue)) {
            Install-PackageProvider -Name NuGet -MinimumVersion 2.8.5.201 -Scope $Scope -Force | Out-Null
        }
        if ((Get-PSRepository -Name PSGallery -ErrorAction SilentlyContinue).InstallationPolicy -ne 'Trusted') {
            Set-PSRepository -Name PSGallery -InstallationPolicy Trusted -ErrorAction SilentlyContinue
        }
        Install-Module -Name MicrosoftPowerBIMgmt -Scope $Scope -Force -AllowClobber
    }
    Import-Module MicrosoftPowerBIMgmt -ErrorAction Stop

    # A full service principal supplied on the command line signs in with no prompt.
    if ($ServicePrincipalId -and $ServicePrincipalSecret -and $TenantId) {
        Write-Host "Signing in with the supplied service principal..." -ForegroundColor Cyan
        $sec = ConvertTo-SecureString $ServicePrincipalSecret -AsPlainText -Force
        Connect-ServicePrincipalIdentity -TenantId $TenantId -AppId $ServicePrincipalId -Secret $sec
        return
    }

    # If there's already a cached session, offer to reuse it or switch identity.
    $current = $null
    try { $current = Get-TokenIdentity (Get-PowerBIAccessToken -AsString -ErrorAction Stop) } catch { }
    if ($current) {
        $ans = Read-Host "Already signed in as '$current'. Press Enter to use this account, or type 's' to sign in as someone else"
        if ($ans -notmatch '^(?i)s') { return }
        try { Disconnect-PowerBIServiceAccount -ErrorAction SilentlyContinue | Out-Null } catch { }
    }

    # Prompt for which identity / sign-in method to use.
    Write-Host ""
    Write-Host "How would you like to sign in?" -ForegroundColor Cyan
    Write-Host "  [1] Interactive user sign-in (browser account picker)   [default]"
    Write-Host "  [2] Service principal (app registration)"
    $choice = Read-Host "Selection"

    if ($choice -eq '2') {
        $tid = Read-Host "  Tenant ID (GUID)"
        $app = Read-Host "  Application (client) ID (GUID)"
        $sec = Read-Host "  Client secret" -AsSecureString
        Connect-ServicePrincipalIdentity -TenantId $tid -AppId $app -Secret $sec
    }
    else {
        Write-Host "Opening interactive sign-in - pick the account to use in the browser window..." -ForegroundColor Cyan
        Connect-PowerBIServiceAccount | Out-Null
    }

    # Confirm the resulting identity.
    try {
        $who = Get-TokenIdentity (Get-PowerBIAccessToken -AsString -ErrorAction Stop)
        if ($who) { Write-Host ("Signed in as: {0}" -f $who) -ForegroundColor Green }
    } catch { }
}

# ---------------------------------------------------------------------------
# Auth: return an "Authorization" header value ("Bearer eyJ...").
# Called on every request so the underlying module can refresh an expiring
# token during a long-running export.
# ---------------------------------------------------------------------------
function Get-AuthHeaderValue {
    if ($script:AccessToken) {
        if ($script:AccessToken -match '^(?i)bearer\s') { return $script:AccessToken }
        return "Bearer $($script:AccessToken)"
    }
    try {
        return (Get-PowerBIAccessToken -AsString -ErrorAction Stop)   # returns "Bearer ..."
    }
    catch {
        throw "No token available. Either pass -AccessToken, or install MicrosoftPowerBIMgmt " +
              "and run Connect-PowerBIServiceAccount as a tenant admin first. ($($_.Exception.Message))"
    }
}

# ---------------------------------------------------------------------------
# Extract HTTP status code from a caught web exception (5.1 + 7 compatible).
# ---------------------------------------------------------------------------
function Get-StatusCode {
    param($Exception)
    try {
        if ($Exception.Response -and $null -ne $Exception.Response.StatusCode) {
            return [int]$Exception.Response.StatusCode
        }
    } catch { }
    return $null
}

# ---------------------------------------------------------------------------
# Extract Retry-After (seconds) from a caught web exception, trying every
# header shape used across PowerShell 5.1 (HttpWebResponse) and 7+
# (HttpResponseMessage). Falls back to $Default if not present/parseable.
# ---------------------------------------------------------------------------
function Get-RetryAfterSeconds {
    param($Exception, [int]$Default)
    $raw = $null
    try {
        $resp = $Exception.Response
        if ($null -ne $resp) {
            # PS 5.1 WebHeaderCollection indexer
            try { $raw = $resp.Headers['Retry-After'] } catch { }
            # PS 7 HttpResponseMessage.Headers.RetryAfter.Delta
            if (-not $raw) {
                try {
                    if ($resp.Headers.RetryAfter -and $resp.Headers.RetryAfter.Delta) {
                        $raw = [int][math]::Ceiling($resp.Headers.RetryAfter.Delta.TotalSeconds)
                    }
                } catch { }
            }
            # PS 7 header collection GetValues
            if (-not $raw) {
                try { $raw = ($resp.Headers.GetValues('Retry-After'))[0] } catch { }
            }
        }
    } catch { }

    if ($raw) {
        $parsed = 0
        if ([int]::TryParse([string]$raw, [ref]$parsed) -and $parsed -gt 0) { return $parsed }
    }
    return $Default
}

# ---------------------------------------------------------------------------
# GET a URI as JSON with retry / 429 / backoff. Returns the parsed object.
# ---------------------------------------------------------------------------
function Invoke-AdminApi {
    param(
        [Parameter(Mandatory)] [string] $Uri,
        [int] $MaxRetries = 6,
        [int] $BaseBackoffSeconds = 5,
        [int] $MaxBackoffSeconds  = 300
    )

    $attempt = 0
    while ($true) {
        $attempt++
        try {
            $headers = @{ Authorization = (Get-AuthHeaderValue) }
            $resp = Invoke-WebRequest -Uri $Uri -Headers $headers -Method Get `
                        -UseBasicParsing -ErrorAction Stop
            return ($resp.Content | ConvertFrom-Json)
        }
        catch {
            $status     = Get-StatusCode -Exception $_.Exception
            $isThrottle = ($status -eq 429)
            $isServer   = ($status -ge 500 -and $status -lt 600)
            $isNetwork  = ($null -eq $status)   # DNS/connection reset/timeout etc.
            $transient  = $isThrottle -or $isServer -or $isNetwork

            if (-not $transient -or $attempt -gt $MaxRetries) {
                $code = if ($status) { "HTTP $status" } else { 'network error' }
                throw "Request to '$Uri' failed after $($attempt-1) retr$(if($attempt-1 -eq 1){'y'}else{'ies'}) ($code): $($_.Exception.Message)"
            }

            # Prefer server-provided Retry-After (esp. for 429); else exponential backoff.
            $defaultWait = [math]::Min($MaxBackoffSeconds, $BaseBackoffSeconds * [math]::Pow(2, $attempt - 1))
            $wait = if ($isThrottle) { Get-RetryAfterSeconds -Exception $_.Exception -Default $defaultWait }
                    else            { $defaultWait }
            $wait = [int]$wait + (Get-Random -Minimum 0 -Maximum 4)   # jitter

            $reason = if ($isThrottle) { 'throttled (429)' } elseif ($isServer) { "server error ($status)" } else { 'network error' }
            Write-Warning ("  {0}; retry {1}/{2} in {3}s..." -f $reason, $attempt, $MaxRetries, $wait)
            Start-Sleep -Seconds $wait
        }
    }
}

# ===========================================================================
# MAIN
# ===========================================================================

# --- Auth bootstrap: raw token, or install-module + sign-in ------------------
$script:AccessToken = $AccessToken
if (-not $AccessToken -and -not $SkipModuleSetup) {
    Initialize-PowerBIConnection -Scope $ModuleScope -TenantId $TenantId `
        -ServicePrincipalId $ServicePrincipalId -ServicePrincipalSecret $ServicePrincipalSecret
}

Write-Host ""
Write-Host "Fabric workspace export" -ForegroundColor Cyan
Write-Host ("  Endpoint : {0}/admin/workspaces" -f $ApiRoot)
Write-Host ("  Output   : {0}" -f $OutputPath)
Write-Host ("  Batch    : flush every {0} workspaces" -f $BatchSize)
Write-Host ""

# Start clean so we always write a fresh header, then append each batch.
if (Test-Path -LiteralPath $OutputPath) { Remove-Item -LiteralPath $OutputPath -Force }
$outDir = Split-Path -Parent $OutputPath
if ($outDir -and -not (Test-Path -LiteralPath $outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
}

# --- 1. Build the capacity lookup (capacityId -> name + sku) -----------------
# Resolved from the Power BI admin capacities API (tenant-wide list). The Fabric
# admin workspaces list only carries capacityId, not the capacity name or SKU.
Write-Host "Fetching capacities..." -ForegroundColor Cyan
$capMap = @{}
try {
    $capUri = "$PowerBiApiRoot/admin/capacities"
    do {
        $capPage  = Invoke-AdminApi -Uri $capUri -MaxRetries $MaxRetries
        $capItems = if ($null -ne $capPage.value)          { $capPage.value }
                    elseif ($null -ne $capPage.capacities)  { $capPage.capacities }
                    else { @() }
        foreach ($c in $capItems) {
            if ($c.id) {
                $capMap[[string]$c.id.ToString().ToLowerInvariant()] = [pscustomobject]@{
                    Name = $c.displayName
                    Sku  = $c.sku
                }
            }
        }
        $capUri = $capPage.continuationUri   # Power BI returns all at once; guard anyway
    } while ($capUri)

    if ($capMap.Count -eq 0) {
        Write-Warning "No capacities returned - capacity columns will be blank. Confirm you signed in as a tenant admin (Tenant.Read.All)."
    }
    else {
        Write-Host ("  {0} capacit{1} loaded." -f $capMap.Count, $(if($capMap.Count -eq 1){'y'}else{'ies'})) -ForegroundColor Green
    }
}
catch {
    Write-Warning "Could not load capacities; capacity columns will be blank. $($_.Exception.Message)"
}
Write-Host ""

# --- 2. Stream workspaces via the Fabric admin API, flushing each batch ------
Write-Host "Fetching workspaces..." -ForegroundColor Cyan

$script:batchNo   = 0
$script:totalRows = 0
$script:buffer    = New-Object System.Collections.Generic.List[object]
$script:missingCapIds  = New-Object System.Collections.Generic.HashSet[string]
$script:missingCapRows = 0

# Append the current buffer to the CSV, then clear it (a "batch").
function Write-Batch {
    if ($script:buffer.Count -eq 0) { return }
    $script:buffer | Export-Csv -LiteralPath $OutputPath -NoTypeInformation -Append -Encoding UTF8
    $script:batchNo++
    $script:totalRows += $script:buffer.Count
    Write-Host ("  Batch {0}: +{1,-5} (running total {2}) written." -f $script:batchNo, $script:buffer.Count, $script:totalRows) -ForegroundColor Green
    Write-Progress -Activity "Exporting Fabric workspaces" `
                   -Status ("Batch {0} - {1} workspaces exported" -f $script:batchNo, $script:totalRows)
    $script:buffer.Clear()
}

$wsUri = "$ApiRoot/admin/workspaces"
do {
    $page  = Invoke-AdminApi -Uri $wsUri -MaxRetries $MaxRetries
    # The newer API returns items under 'workspaces'; tolerate 'value' too.
    $items = if ($null -ne $page.workspaces) { $page.workspaces }
             elseif ($null -ne $page.value)  { $page.value }
             else { @() }

    foreach ($w in $items) {
        $capName = ''
        $capSku  = ''
        if ($w.capacityId) {
            $key = [string]$w.capacityId.ToString().ToLowerInvariant()
            if ($capMap.ContainsKey($key)) {
                $cap     = $capMap[$key]
                $capName = $cap.Name
                $capSku  = $cap.Sku
            }
            else {
                # capacityId present but not in the capacity list - track for a summary.
                [void]$script:missingCapIds.Add($key)
                $script:missingCapRows++
            }
        }

        # 'name' on the Fabric API; coalesce to displayName for older shapes.
        $wsName = if ($w.name) { $w.name } else { $w.displayName }

        $script:buffer.Add([pscustomobject][ordered]@{
            'ID'                = $w.id
            'Name'              = $wsName
            'Description'       = $w.description   # not returned by this API - stays blank
            'Type'              = $w.type
            'State'             = $w.state
            'Capacity name'     = $capName
            'Capacity SKU Tier' = $capSku
        })

        if ($script:buffer.Count -ge $BatchSize) { Write-Batch }
    }

    # Follow the continuation to the next page. Prefer the ready-made URI; the raw
    # token is already URL-encoded, so append it as-is (do NOT re-encode).
    $wsUri = $page.continuationUri
    if (-not $wsUri -and $page.continuationToken) {
        $wsUri = "$ApiRoot/admin/workspaces?continuationToken=$($page.continuationToken)"
    }
}
while ($wsUri)

Write-Batch   # final partial batch
$totalRows = $script:totalRows
Write-Progress -Activity "Exporting Fabric workspaces" -Completed

if ($script:missingCapIds.Count -gt 0) {
    Write-Warning ("{0} workspace(s) reference {1} capacit(y/ies) not in the capacity list; their capacity columns are blank." -f $script:missingCapRows, $script:missingCapIds.Count)
}

Write-Host ""
if ($totalRows -eq 0) {
    Write-Warning "No workspaces were returned. Check that you signed in as a tenant admin (Tenant.Read.All)."
}
else {
    $finalPath = if (Test-Path -LiteralPath $OutputPath) { (Resolve-Path -LiteralPath $OutputPath).Path } else { $OutputPath }
    Write-Host ("Done. {0} workspaces exported to {1}" -f $totalRows, $finalPath) -ForegroundColor Cyan
}
