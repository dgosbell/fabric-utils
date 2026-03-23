<#
.SYNOPSIS
    Scans all Power BI reports across a Microsoft Fabric tenant to identify custom visuals.

.DESCRIPTION
    This script uses the Microsoft Fabric REST APIs to:
    1. List all workspaces in the tenant (Admin API)
    2. List all Power BI reports (Admin API)
    3. Retrieve report definitions (Get Item Definition or Bulk Export)
    4. Parse definitions (PBIR and PBIR-Legacy formats) to extract custom visuals
    5. Output a CSV with report details and custom visual information

    The executing identity must be a Fabric Administrator. The script can optionally
    add/remove the executing user as a workspace admin to access report definitions.

    Personal workspaces ("My Workspace") are flagged in the output but cannot be scanned
    for definitions (tenant admins cannot add themselves to personal workspaces).

.PARAMETER OutputPath
    Path to the output CSV file. Defaults to "CustomVisuals_<timestamp>.csv" in current directory.

.PARAMETER ErrorLogPath
    Path to the error log file. Defaults to "CustomVisuals_Errors_<timestamp>.log" in current directory.

.PARAMETER AddSelfToWorkspaces
    When specified, the script will temporarily add the executing user as an Admin to
    workspaces where access is needed, and remove them after processing.

.PARAMETER WorkspaceFilter
    Optional filter to limit scanning to specific workspace names (supports wildcards).

.PARAMETER UseBulkExport
    When specified, uses the Bulk Export Item Definitions (beta) API instead of
    individual Get Item Definition calls. Can be faster but is a beta feature.

.EXAMPLE
    .-FabricCustomVisuals -AddSelfToWorkspaces
    .-FabricCustomVisuals -OutputPath "C:\Reports\visuals.csv" -WorkspaceFilter "Sales*"
    .-FabricCustomVisuals -UseBulkExport -AddSelfToWorkspaces
#>

[CmdletBinding()]
param(
    [string]$OutputPath,
    [string]$ErrorLogPath,
    [string]$LogPath,
    [switch]$AddSelfToWorkspaces,
    [string]$WorkspaceFilter,
    [switch]$UseBulkExport
)

#region --- Configuration ---
$FabricApiBase = "https://api.fabric.microsoft.com/v1"
$PowerBIApiBase = "https://api.powerbi.com/v1.0/myorg"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

if (-not $OutputPath) {
    $OutputPath = Join-Path $PSScriptRoot "CustomVisuals_$timestamp.csv"
}
if (-not $ErrorLogPath) {
    $ErrorLogPath = Join-Path $PSScriptRoot "CustomVisuals_Errors_$timestamp.log"
}
if (-not $LogPath) {
    $LogPath = Join-Path $PSScriptRoot "CustomVisuals_Log_$timestamp.log"
}

# AppSource custom visuals lookup URL
$AppSourceVisualsUrl = "https://raw.githubusercontent.com/DataChant/PowerBI-Visuals-AppSource/refs/heads/main/Visuals%20Summary.csv"

#endregion

#region --- Helper Functions ---

function Write-Log {
    <#
    .SYNOPSIS
        Writes a timestamped entry to the activity log file.
    #>
    param(
        [string]$Message,
        [ValidateSet('INFO','WARN','ERROR','ACCESS','DEBUG')]
        [string]$Level = 'INFO'
    )
    # DEBUG level only logs when -Verbose is active
    if ($Level -eq 'DEBUG' -and -not $VerbosePreference -eq 'Continue') {
        return
    }
    $logEntry = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] [$Level] $Message"
    Add-Content -Path $LogPath -Value $logEntry -ErrorAction SilentlyContinue
    if ($Level -eq 'WARN') {
        Write-Verbose $Message
    }
    elseif ($Level -eq 'ERROR') {
        Write-Verbose $Message
    }
    elseif ($Level -eq 'DEBUG') {
        Write-Verbose $Message
    }
}

function Write-ErrorLog {
    param([string]$Message)
    $logEntry = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
    Add-Content -Path $ErrorLogPath -Value $logEntry -ErrorAction SilentlyContinue
    Write-Log -Message $Message -Level 'ERROR'
    Write-Warning $Message
}


function Write-CsvRow {
    <#
    .SYNOPSIS
        Appends a single result row to the output CSV file. Creates file with headers on first call.
    #>
    param([PSCustomObject]$Row)
    if (-not (Test-Path $OutputPath)) {
        $Row | Export-Csv -Path $OutputPath -NoTypeInformation -Encoding UTF8 -Force
    }
    else {
        $Row | Export-Csv -Path $OutputPath -Append -NoTypeInformation -Encoding UTF8 -Force
    }
}
function Get-FabricToken {
    <#
    .SYNOPSIS
        Obtains an access token for the Fabric API using Az.Accounts.
        The Fabric API requires a token with audience https://api.fabric.microsoft.com
        which is different from the Power BI audience used by MicrosoftPowerBIMgmt.
    #>
    try {
        $tokenObj = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com" -ErrorAction Stop
        # Az.Accounts 4.x+ returns SecureString Token; older versions return plain text
        if ($tokenObj.Token -is [System.Security.SecureString]) {
            return $tokenObj.Token | ConvertFrom-SecureString -AsPlainText
        }
        elseif ($tokenObj.Token) {
            return $tokenObj.Token
        }
        elseif ($tokenObj.AccessToken) {
            return $tokenObj.AccessToken
        }
        else {
            return $tokenObj.ToString()
        }
    }
    catch {
        throw "Failed to obtain Fabric API access token. Ensure you are logged in with Connect-AzAccount. Error: $_"
    }
}

function Get-PowerBIToken {
    <#
    .SYNOPSIS
        Obtains an access token for the Power BI API (api.powerbi.com).
        Uses the Power BI audience https://analysis.windows.net/powerbi/api.
    #>
    try {
        $tokenObj = Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api" -ErrorAction Stop
        if ($tokenObj.Token -is [System.Security.SecureString]) {
            return $tokenObj.Token | ConvertFrom-SecureString -AsPlainText
        }
        elseif ($tokenObj.Token) {
            return $tokenObj.Token
        }
        elseif ($tokenObj.AccessToken) {
            return $tokenObj.AccessToken
        }
        else {
            return $tokenObj.ToString()
        }
    }
    catch {
        throw "Failed to obtain Power BI API access token. Error: $_"
    }
}

function Get-AuthHeaders {
    param([string]$Token)
    return @{
        "Authorization" = "Bearer $Token"
        "Content-Type"  = "application/json"
    }
}

function Invoke-FabricApi {
    <#
    .SYNOPSIS
        Calls a Fabric/PowerBI REST API with retry logic for throttling.
        Handles both HTTP 429 and Fabric's JSON-body RequestBlocked errors.
    #>
    param(
        [string]$Uri,
        [string]$Method = "GET",
        [object]$Body,
        [hashtable]$Headers,
        [int]$MaxRetries = 5
    )

    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $params = @{
                Uri     = $Uri
                Method  = $Method
                Headers = $Headers
                ErrorAction = "Stop"
            }
            if ($Body) {
                if ($Body -is [string]) {
                    $params["Body"] = $Body
                }
                else {
                    $params["Body"] = ($Body | ConvertTo-Json -Depth 10)
                }
                Write-Log -Message "$Method $Uri`nBody: $($params["Body"])" -Level 'DEBUG'
            }
            else {
                Write-Log -Message "$Method $Uri" -Level 'DEBUG'
            }

            $response = Invoke-RestMethod @params
            return $response
        }
        catch {
            $statusCode = $null
            if ($_.Exception.Response) {
                $statusCode = [int]$_.Exception.Response.StatusCode
            }

            # Try to parse the error body for Fabric-specific error codes
            $errorBody = $null
            try {
                $errorStream = $_.Exception.Response.GetResponseStream()
                if ($errorStream) {
                    $reader = [System.IO.StreamReader]::new($errorStream)
                    $errorText = $reader.ReadToEnd()
                    $reader.Close()
                    if ($errorText) {
                        $errorBody = $errorText | ConvertFrom-Json -ErrorAction SilentlyContinue
                    }
                }
            }
            catch { }
            # Also try the error message itself (PowerShell sometimes puts the body there)
            if (-not $errorBody) {
                try {
                    $msg = $_.ErrorDetails.Message
                    if ($msg) { $errorBody = $msg | ConvertFrom-Json -ErrorAction SilentlyContinue }
                }
                catch { }
            }

            $fabricErrorCode = if ($errorBody -and $errorBody.errorCode) { $errorBody.errorCode } else { "" }
            $isRetriable = if ($errorBody -and $null -ne $errorBody.isRetriable) { $errorBody.isRetriable } else { $false }

            # Handle throttling: HTTP 429 OR Fabric's RequestBlocked
            if ($statusCode -eq 429 -or $fabricErrorCode -eq "RequestBlocked") {
                $retryAfter = 60
                # Try Retry-After header
                try {
                    $retryHeader = $_.Exception.Response.Headers | Where-Object { $_.Key -eq "Retry-After" }
                    if ($retryHeader) { $retryAfter = [int]$retryHeader.Value[0] }
                }
                catch { }
                # Try parsing the blocked-until timestamp from the Fabric error message
                if ($errorBody -and $errorBody.message -match 'until:\s*(.+)\s*\(UTC\)') {
                    try {
                        $blockedUntil = [DateTimeOffset]::Parse($Matches[1] + " +00:00")
                        $waitSeconds = [Math]::Max(5, [Math]::Ceiling(($blockedUntil - [DateTimeOffset]::UtcNow).TotalSeconds))
                        $retryAfter = [Math]::Min($waitSeconds + 5, 300)  # Cap at 5 minutes, add 5s buffer
                    }
                    catch { }
                }
                Write-Warning "Throttled ($fabricErrorCode). Waiting $retryAfter seconds before retry $attempt/$MaxRetries..."
                Write-Log -Message "Throttled on $Uri - waiting $retryAfter seconds (attempt $attempt/$MaxRetries)" -Level 'WARN'
                Start-Sleep -Seconds $retryAfter
                continue
            }
            elseif ($statusCode -eq 403) {
                throw $_
            }
            elseif ($isRetriable -and $attempt -lt $MaxRetries) {
                $waitSec = [Math]::Pow(2, $attempt) * 5
                Write-Warning "Retriable error '$fabricErrorCode' (attempt $attempt/$MaxRetries). Waiting $waitSec seconds..."
                Start-Sleep -Seconds $waitSec
                continue
            }
            elseif ($attempt -lt $MaxRetries -and $statusCode -ge 500) {
                $waitSec = [Math]::Pow(2, $attempt) * 5
                Write-Verbose "Server error $statusCode (attempt $attempt/$MaxRetries). Waiting $waitSec seconds..."
                Start-Sleep -Seconds $waitSec
                continue
            }
            else {
                throw $_
            }
        }
    }
}

function Invoke-FabricApiRaw {
    <#
    .SYNOPSIS
        Calls a Fabric REST API returning the raw web response (for handling 202 LRO).
    #>
    param(
        [string]$Uri,
        [string]$Method = "POST",
        [object]$Body,
        [hashtable]$Headers,
        [int]$MaxRetries = 3
    )

    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $params = @{
                Uri                  = $Uri
                Method               = $Method
                Headers              = $Headers
                ErrorAction          = "Stop"
                ResponseHeadersVariable = "responseHeaders"
                StatusCodeVariable   = "statusCode"
            }
            if ($Body) {
                if ($Body -is [string]) {
                    $params["Body"] = $Body
                }
                else {
                    $params["Body"] = ($Body | ConvertTo-Json -Depth 10)
                }
            }

            $response = Invoke-WebRequest @params
            return @{
                StatusCode = $response.StatusCode
                Headers    = $response.Headers
                Content    = $response.Content
            }
        }
        catch {
            $statusCode = $null
            if ($_.Exception.Response) {
                $statusCode = [int]$_.Exception.Response.StatusCode
            }

            if ($statusCode -eq 429) {
                $retryAfter = 60
                Write-Warning "Throttled (429). Waiting $retryAfter seconds before retry $attempt/$MaxRetries..."
                Start-Sleep -Seconds $retryAfter
                continue
            }
            elseif ($statusCode -eq 403) {
                throw $_
            }
            elseif ($statusCode -eq 202) {
                # Long-running operation - return the response with headers
                return @{
                    StatusCode = 202
                    Headers    = $_.Exception.Response.Headers
                    Content    = $null
                }
            }
            elseif ($attempt -lt $MaxRetries) {
                $waitSec = [Math]::Pow(2, $attempt) * 5
                Write-Verbose "API error (attempt $attempt/$MaxRetries). Waiting $waitSec seconds..."
                Start-Sleep -Seconds $waitSec
                continue
            }
            else {
                throw $_
            }
        }
    }
}

function Wait-LongRunningOperation {
    <#
    .SYNOPSIS
        Polls a Fabric long-running operation URL until completion, then fetches the result.
        Fabric LRO pattern: poll state URL until status=Succeeded, then GET {url}/result.
    #>
    param(
        [string]$OperationUrl,
        [hashtable]$Headers,
        [int]$MaxWaitSeconds = 600,
        [int]$DefaultPollSeconds = 10
    )

    $elapsed = 0
    while ($elapsed -lt $MaxWaitSeconds) {
        Start-Sleep -Seconds $DefaultPollSeconds
        $elapsed += $DefaultPollSeconds

        try {
            $response = Invoke-WebRequest -Uri $OperationUrl -Method GET -Headers $Headers -ErrorAction Stop
            $body = $response.Content | ConvertFrom-Json

            # Check if this is an operation status response (has 'status' property)
            if ($null -ne $body.status) {
                Write-Verbose "LRO status: $($body.status), $($body.percentComplete)% complete ($elapsed seconds elapsed)"

                if ($body.status -eq "Succeeded") {
                    # Operation complete - fetch the actual result from {operationUrl}/result
                    $resultUrl = "$OperationUrl/result"
                    Write-Verbose "LRO succeeded. Fetching result from: $resultUrl"
                    try {
                        $resultResponse = Invoke-WebRequest -Uri $resultUrl -Method GET -Headers $Headers -ErrorAction Stop
                        return ($resultResponse.Content | ConvertFrom-Json)
                    }
                    catch {
                        # Some operations don't have a /result endpoint - return the status object
                        Write-Verbose "Could not fetch /result, returning status object"
                        return $body
                    }
                }
                elseif ($body.status -eq "Failed") {
                    $errorMsg = if ($body.error) { $body.error | ConvertTo-Json -Compress } else { "Unknown error" }
                    throw "Long-running operation failed: $errorMsg"
                }
                # Still running - continue polling
                continue
            }
            else {
                # Response doesn't have a status property - it IS the result
                return $body
            }
        }
        catch {
            if ($_.Exception.Message -match "Long-running operation failed") {
                throw
            }
            $sc = $null
            try { $sc = [int]$_.Exception.Response.StatusCode } catch { }
            if ($sc -eq 202) {
                Write-Verbose "LRO still running (202)... ($elapsed seconds elapsed)"
                continue
            }
            else {
                throw "Long-running operation polling failed with status $sc`: $_"
            }
        }
    }

    throw "Long-running operation timed out after $MaxWaitSeconds seconds."
}

function Get-CurrentUserObjectId {
    <#
    .SYNOPSIS
        Gets the object ID of the currently logged-in user from the access token.
    #>
    param([string]$Token)

    try {
        # Decode JWT payload
        $parts = $Token.Split('.')
        $payload = $parts[1]
        # Pad base64 if needed
        $padding = 4 - ($payload.Length % 4)
        if ($padding -ne 4) { $payload += ('=' * $padding) }
        $decoded = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($payload))
        $claims = $decoded | ConvertFrom-Json
        return $claims.oid
    }
    catch {
        Write-ErrorLog "Could not extract user object ID from token: $_"
        return $null
    }
}

function Get-CurrentUserEmail {
    param([string]$Token)
    try {
        $parts = $Token.Split('.')
        $payload = $parts[1]
        $padding = 4 - ($payload.Length % 4)
        if ($padding -ne 4) { $payload += ('=' * $padding) }
        $decoded = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($payload))
        $claims = $decoded | ConvertFrom-Json
        return $claims.upn
    }
    catch {
        return $null
    }
}

#endregion

#region --- Core Functions ---

function Get-AllWorkspaces {
    <#
    .SYNOPSIS
        Lists all workspaces in the tenant using the Admin API with pagination.
    #>
    param([hashtable]$Headers)

    $workspaces = [System.Collections.Generic.List[object]]::new()
    $uri = "$FabricApiBase/admin/workspaces?state=Active"

    do {
        Write-Verbose "Fetching workspaces: $uri"
        $response = Invoke-FabricApi -Uri $uri -Headers $Headers

        if ($response.workspaces) {
            foreach ($ws in $response.workspaces) {
                $workspaces.Add($ws)
            }
        }

        $uri = $null
        if ($response.continuationUri) {
            $uri = $response.continuationUri
        }
    } while ($uri)

    return $workspaces
}

function Get-AllReports {
    <#
    .SYNOPSIS
        Lists all Power BI reports in the tenant using the Power BI Admin API.
        Uses the PBI Admin API because it returns reportType (PowerBIReport vs PaginatedReport),
        allowing us to filter out paginated reports which don't support custom visuals.
    #>
    param(
        [hashtable]$FabricHeaders,
        [hashtable]$PbiHeaders
    )

    $reports = [System.Collections.Generic.List[object]]::new()

    # Use Power BI Admin API to get reports with reportType field
    $uri = "$PowerBIApiBase/admin/reports"
    $hasMore = $true
    $skip = 0
    $top = 5000

    while ($hasMore) {
        $pageUri = "$uri`?`$top=$top&`$skip=$skip"
        Write-Verbose "Fetching reports: $pageUri"
        $response = Invoke-FabricApi -Uri $pageUri -Headers $PbiHeaders

        if ($response.value) {
            foreach ($item in $response.value) {
                # Only include Power BI reports - skip paginated reports, scorecards, dashboards, etc.
                if ($item.reportType -ne "PowerBIReport") {
                    Write-Log -Message "  Skipping non-report item: '$($item.name)' (Id: $($item.id), Type: $($item.reportType), Workspace: $($item.workspaceId))" -Level 'DEBUG'
                    continue
                }
                Write-Log -Message "  Report: '$($item.name)' (Id: $($item.id), Workspace: $($item.workspaceId), Type: $($item.reportType), AppId: $($item.appId))" -Level 'DEBUG'
                $reports.Add($item)
            }

            if ($response.value.Count -lt $top) {
                $hasMore = $false
            } else {
                $skip += $top
            }
        } else {
            $hasMore = $false
        }
    }

    return $reports
}

function Add-SelfToWorkspace {
    <#
    .SYNOPSIS
        Adds the current user as Admin to a workspace using the Power BI Admin API.
    #>
    param(
        [string]$WorkspaceId,
        [string]$WorkspaceName,
        [string]$UserEmail,
        [hashtable]$Headers
    )

    $uri = "$PowerBIApiBase/admin/groups/$WorkspaceId/users"
    $body = @{
        emailAddress         = $UserEmail
        groupUserAccessRight = "Admin"
        principalType        = "User"
    }

    try {
        Invoke-FabricApi -Uri $uri -Method "POST" -Body $body -Headers $Headers
        Write-Log -Message "ACCESS GRANTED: Added '$UserEmail' as Admin to workspace '$WorkspaceName' ($WorkspaceId)" -Level 'ACCESS'
        return $true
    }
    catch {
        Write-ErrorLog "Failed to add self to workspace '$WorkspaceName' ($WorkspaceId): $_"
        return $false
    }
}

function Remove-SelfFromWorkspace {
    <#
    .SYNOPSIS
        Removes the current user from a workspace using the Power BI Admin API.
    #>
    param(
        [string]$WorkspaceId,
        [string]$WorkspaceName,
        [string]$UserEmail,
        [hashtable]$Headers
    )

    $uri = "$PowerBIApiBase/admin/groups/$WorkspaceId/users/$UserEmail"
    try {
        Invoke-FabricApi -Uri $uri -Method "DELETE" -Headers $Headers
        Write-Log -Message "ACCESS REVOKED: Removed '$UserEmail' from workspace '$WorkspaceName' ($WorkspaceId)" -Level 'ACCESS'
    }
    catch {
        Write-ErrorLog "Failed to remove self from workspace '$WorkspaceName' ($WorkspaceId): $_"
    }
}

function Get-ReportDefinition {
    <#
    .SYNOPSIS
        Gets the definition of a single report using the Fabric Core API.
        Includes retry logic for throttling (RequestBlocked) and LRO (202) handling.
    #>
    param(
        [string]$WorkspaceId,
        [string]$ReportId,
        [hashtable]$Headers
    )

    $uri = "$FabricApiBase/workspaces/$WorkspaceId/items/$ReportId/getDefinition"
    $maxRetries = 5

    for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
        try {
            Write-Log -Message "POST $uri" -Level 'DEBUG'
            $response = Invoke-WebRequest -Uri $uri -Method POST -Headers $Headers -ErrorAction Stop

            if ($response.StatusCode -eq 202) {
                # Long-running operation - extract Location header and poll
                $location = $response.Headers['Location']
                if ($location -is [array]) { $location = $location[0] }
                if ($location) {
                    Write-Verbose "Report definition is a long-running operation. Polling: $location"
                    return Wait-LongRunningOperation -OperationUrl $location -Headers $Headers
                }
                return $null
            }

            # 200 OK
            return ($response.Content | ConvertFrom-Json)
        }
        catch {
            $sc = $null
            if ($_.Exception.Response) { $sc = [int]$_.Exception.Response.StatusCode }

            $errorBody = $null
            try { $msg = $_.ErrorDetails.Message; if ($msg) { $errorBody = $msg | ConvertFrom-Json -ErrorAction SilentlyContinue } } catch { }

            $fabricErrorCode = if ($errorBody -and $errorBody.errorCode) { $errorBody.errorCode } else { "" }
            $isRetriable = if ($errorBody -and $null -ne $errorBody.isRetriable) { $errorBody.isRetriable } else { $false }

            if ($sc -eq 429 -or $fabricErrorCode -eq "RequestBlocked") {
                $retryAfter = 60
                if ($errorBody -and $errorBody.message -match 'until:\s*(.+)\s*\(UTC\)') {
                    try {
                        $blockedUntil = [DateTimeOffset]::Parse($Matches[1] + " +00:00")
                        $waitSeconds = [Math]::Max(5, [Math]::Ceiling(($blockedUntil - [DateTimeOffset]::UtcNow).TotalSeconds))
                        $retryAfter = [Math]::Min($waitSeconds + 5, 300)
                    }
                    catch { }
                }
                Write-Warning "Throttled ($fabricErrorCode) on report definition. Waiting $retryAfter seconds (attempt $attempt/$maxRetries)..."
                Write-Log -Message "Throttled getting definition for report $ReportId - waiting $retryAfter seconds (attempt $attempt/$maxRetries)" -Level 'WARN'
                Start-Sleep -Seconds $retryAfter
                continue
            }
            elseif ($sc -eq 403) { throw $_ }
            elseif ($fabricErrorCode -in @("EntityNotFound", "OperationNotSupportedForItem", "ItemNotFound")) {
                throw [System.IO.FileNotFoundException]::new("SkippableItem: $fabricErrorCode - $($errorBody.message)")
            }
            elseif ($isRetriable -and $attempt -lt $maxRetries) {
                $waitSec = [Math]::Pow(2, $attempt) * 5
                Write-Warning "Retriable error '$fabricErrorCode' (attempt $attempt/$maxRetries). Waiting $waitSec seconds..."
                Start-Sleep -Seconds $waitSec
                continue
            }
            else { throw $_ }
        }
    }
}

function Get-BulkReportDefinitions {
    <#
    .SYNOPSIS
        Gets definitions for specific reports in a workspace using the Bulk Export API (beta).
        Uses selective mode to only export the report items we need, not all workspace items.
    #>
    param(
        [string]$WorkspaceId,
        [string[]]$ReportIds,
        [hashtable]$Headers
    )

    $uri = "$FabricApiBase/workspaces/$WorkspaceId/items/bulkExportDefinitions?beta=true"
    $body = @{
        mode  = "Selective"
        items = @($ReportIds | ForEach-Object { @{ id = $_ } })
    }

    $maxRetries = 5
    for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
        try {
            $bodyJson = $body | ConvertTo-Json -Depth 5
            Write-Log -Message "POST $uri`nBody: $bodyJson" -Level 'DEBUG'
            $response = Invoke-WebRequest -Uri $uri -Method POST -Headers $Headers -Body $bodyJson -ErrorAction Stop

            if ($response.StatusCode -eq 202) {
                # Long-running operation - extract Location header and poll
                $location = $response.Headers['Location']
                if ($location -is [array]) { $location = $location[0] }
                if ($location) {
                    Write-Verbose "Bulk export is a long-running operation. Polling: $location"
                    return Wait-LongRunningOperation -OperationUrl $location -Headers $Headers -MaxWaitSeconds 900
                }
                return $null
            }

            # 200 OK
            return ($response.Content | ConvertFrom-Json)
        }
        catch {
            $sc = $null
            if ($_.Exception.Response) { $sc = [int]$_.Exception.Response.StatusCode }

            $errorBody = $null
            try { $msg = $_.ErrorDetails.Message; if ($msg) { $errorBody = $msg | ConvertFrom-Json -ErrorAction SilentlyContinue } } catch { }

            $fabricErrorCode = if ($errorBody -and $errorBody.errorCode) { $errorBody.errorCode } else { "" }
            $isRetriable = if ($errorBody -and $null -ne $errorBody.isRetriable) { $errorBody.isRetriable } else { $false }

            if ($sc -eq 429 -or $fabricErrorCode -eq "RequestBlocked") {
                $retryAfter = 60
                if ($errorBody -and $errorBody.message -match 'until:\s*(.+)\s*\(UTC\)') {
                    try {
                        $blockedUntil = [DateTimeOffset]::Parse($Matches[1] + " +00:00")
                        $waitSeconds = [Math]::Max(5, [Math]::Ceiling(($blockedUntil - [DateTimeOffset]::UtcNow).TotalSeconds))
                        $retryAfter = [Math]::Min($waitSeconds + 5, 300)
                    }
                    catch { }
                }
                Write-Warning "Throttled ($fabricErrorCode). Waiting $retryAfter seconds before retry $attempt/$maxRetries..."
                Write-Log -Message "Bulk export throttled for workspace $WorkspaceId - waiting $retryAfter seconds (attempt $attempt/$maxRetries)" -Level 'WARN'
                Start-Sleep -Seconds $retryAfter
                continue
            }
            elseif ($sc -eq 403) { throw $_ }
            elseif ($fabricErrorCode -in @("UnknownError", "EntityNotFound", "ItemNotFound")) {
                throw [System.InvalidOperationException]::new("BulkExportFailed: $fabricErrorCode - $($errorBody.message)")
            }
            elseif ($isRetriable -and $attempt -lt $maxRetries) {
                $waitSec = [Math]::Pow(2, $attempt) * 5
                Write-Warning "Retriable error '$fabricErrorCode' (attempt $attempt/$maxRetries). Waiting $waitSec seconds..."
                Start-Sleep -Seconds $waitSec
                continue
            }
            else { throw $_ }
        }
    }
}
function Extract-CustomVisualsFromDefinition {
    <#
    .SYNOPSIS
        Parses report definition parts to extract custom visuals with page-level usage.
        Detects AppSource visuals from publicCustomVisuals array in report.json,
        and private/org visuals from resourcePackages with type "CustomVisual".
        For PBIR-Legacy: walks sections/visualContainers matching visualType to custom visual GUIDs.
        For PBIR: scans visual.json files matching visualType to custom visual GUIDs.
    #>
    param(
        [object]$Definition,
        [string]$ReportId,
        [hashtable]$AppSourceLookup
    )

    $customVisuals = [System.Collections.Generic.List[object]]::new()
    $format = "Unknown"

    $parts = $null
    if ($Definition.definition -and $Definition.definition.parts) { $parts = $Definition.definition.parts }
    elseif ($Definition.parts) { $parts = $Definition.parts }
    elseif ($Definition.definitionParts) { $parts = $Definition.definitionParts }

    if (-not $parts) {
        Write-Verbose "No definition parts found for report $ReportId"
        return @{ Visuals = $customVisuals; Format = $format }
    }

    # ================================================================
    # Pass 1: Find report.json and build the set of known custom visuals
    # ================================================================
    $knownCustomVisuals = @{}
    $reportJson = $null

    foreach ($part in $parts) {
        if (-not $part.path -or -not $part.payload) { continue }
        if ($part.path -notmatch 'report\.json$' -or $part.path -match 'definition\.pbir') { continue }

        try {
            $jsonText = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($part.payload))
            $reportJson = $jsonText | ConvertFrom-Json -ErrorAction Stop
        }
        catch { continue }

        if ($reportJson.sections) { $format = "PBIR-Legacy" } else { $format = "PBIR" }

        # AppSource visuals
        if ($reportJson.publicCustomVisuals) {
            foreach ($vg in $reportJson.publicCustomVisuals) {
                $m = @{ DisplayName = $vg; Publisher = ""; Version = ""; IsCertified = "Unknown"; Source = "AppSource"; AppSourceLink = "" }
                if ($AppSourceLookup -and $AppSourceLookup.ContainsKey($vg)) {
                    $info = $AppSourceLookup[$vg]
                    $m.DisplayName = $info.Name; $m.Publisher = $info.Publisher; $m.Version = $info.Version
                    $m.IsCertified = if ($info.IsCertified -eq "Certified") { "Yes" } else { "No" }
                    $m.AppSourceLink = $info.AppSourceLink
                }
                $knownCustomVisuals[$vg] = $m
            }
        }

        # Private/org visuals
        if ($reportJson.resourcePackages) {
            foreach ($pkg in $reportJson.resourcePackages) {
                if ($pkg.type -ne "CustomVisual") { continue }
                $m = @{ DisplayName = $pkg.name; Publisher = ""; Version = ""; IsCertified = "N/A"; Source = "Private"; AppSourceLink = "" }
                if ($AppSourceLookup -and $AppSourceLookup.ContainsKey($pkg.name)) {
                    $info = $AppSourceLookup[$pkg.name]
                    $m.DisplayName = $info.Name; $m.Version = $info.Version
                }
                $knownCustomVisuals[$pkg.name] = $m
            }
        }
        break
    }

    if ($knownCustomVisuals.Count -eq 0) {
        return @{ Visuals = $customVisuals; Format = $format }
    }

    # ================================================================
    # Pass 2: Find page-level usage of each custom visual
    # ================================================================
    $foundOnPages = @{}

    if ($format -eq "PBIR-Legacy" -and $reportJson -and $reportJson.sections) {
        # PBIR-Legacy: walk sections -> visualContainers, match visualType to custom visual GUIDs
        foreach ($section in $reportJson.sections) {
            $pageName = if ($section.displayName) { $section.displayName }
                        elseif ($section.name) { $section.name }
                        else { "Unknown Page" }
            if (-not $section.visualContainers) { continue }

            foreach ($vc in $section.visualContainers) {
                $config = $null
                if ($vc.config -is [string]) {
                    try { $config = $vc.config | ConvertFrom-Json -ErrorAction SilentlyContinue } catch { }
                }
                elseif ($vc.config) { $config = $vc.config }
                if (-not $config -or -not $config.singleVisual) { continue }

                $vt = $config.singleVisual.visualType
                if ($vt -and $knownCustomVisuals.ContainsKey($vt)) {
                    if (-not $foundOnPages.ContainsKey($vt)) { $foundOnPages[$vt] = [System.Collections.Generic.List[string]]::new() }
                    if (-not $foundOnPages[$vt].Contains($pageName)) { $foundOnPages[$vt].Add($pageName) }
                }
            }
        }
    }
    elseif ($format -eq "PBIR") {
        # PBIR: build page lookup from page.json, then scan visual.json for visualType
        $pbirPageLookup = @{}
        foreach ($part in $parts) {
            if ($part.path -match 'pages/([^/]+)/page\.json$') {
                try {
                    $pj = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($part.payload)) | ConvertFrom-Json -ErrorAction SilentlyContinue
                    $pid2 = $Matches[1]
                    if ($pj.displayName) { $pbirPageLookup[$pid2] = $pj.displayName }
                    elseif ($pj.name) { $pbirPageLookup[$pid2] = $pj.name }
                }
                catch { }
            }
        }

        foreach ($part in $parts) {
            if ($part.path -match 'pages/([^/]+)/visuals/[^/]+/visual\.json$') {
                $pageId = $Matches[1]
                $pageName = if ($pbirPageLookup.ContainsKey($pageId)) { $pbirPageLookup[$pageId] } else { $pageId }

                try {
                    $vj = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($part.payload)) | ConvertFrom-Json -ErrorAction SilentlyContinue
                }
                catch { continue }

                $vt = $null
                if ($vj.visual -and $vj.visual.visualType) { $vt = $vj.visual.visualType }
                elseif ($vj.visualType) { $vt = $vj.visualType }

                if ($vt -and $knownCustomVisuals.ContainsKey($vt)) {
                    if (-not $foundOnPages.ContainsKey($vt)) { $foundOnPages[$vt] = [System.Collections.Generic.List[string]]::new() }
                    if (-not $foundOnPages[$vt].Contains($pageName)) { $foundOnPages[$vt].Add($pageName) }
                }
            }
        }
    }

    # ================================================================
    # Pass 3: Build output - one row per visual per page
    # ================================================================
    foreach ($visualGuid in $knownCustomVisuals.Keys) {
        $meta = $knownCustomVisuals[$visualGuid]
        if ($foundOnPages.ContainsKey($visualGuid)) {
            foreach ($pageName in $foundOnPages[$visualGuid]) {
                $customVisuals.Add([PSCustomObject]@{
                    CustomVisualId = $visualGuid; CustomVisualName = $visualGuid
                    CustomVisualDisplayName = $meta.DisplayName; CustomVisualVersion = $meta.Version
                    CustomVisualPublisher = $meta.Publisher; CustomVisualSource = $meta.Source
                    IsCertified = $meta.IsCertified; AppSourceLink = $meta.AppSourceLink
                    PageName = $pageName
                })
            }
        }
        else {
            $customVisuals.Add([PSCustomObject]@{
                CustomVisualId = $visualGuid; CustomVisualName = $visualGuid
                CustomVisualDisplayName = $meta.DisplayName; CustomVisualVersion = $meta.Version
                CustomVisualPublisher = $meta.Publisher; CustomVisualSource = $meta.Source
                IsCertified = $meta.IsCertified; AppSourceLink = $meta.AppSourceLink
                PageName = "(registered but not placed on a page)"
            })
        }
    }

    return @{ Visuals = $customVisuals; Format = $format }
}

#endregion

#region --- Main Script ---

$ErrorActionPreference = "Continue"
# Remove existing output file so first Write-CsvRow creates it with headers
if (Test-Path $OutputPath) { Remove-Item $OutputPath -Force }
$stats = @{
    WorkspacesTotal      = 0
    WorkspacesScanned    = 0
    WorkspacesSkipped    = 0
    WorkspacesAccessErr  = 0
    ReportsTotal         = 0
    ReportsScanned       = 0
    ReportsSkipped       = 0
    ReportsErrored       = 0
    CustomVisualsFound   = 0
}

Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  Power BI Custom Visual Scanner" -ForegroundColor Cyan
Write-Host "  Using Microsoft Fabric REST APIs" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

# Initialize activity log
Write-Log -Message "========== Custom Visual Scanner Started ==========" -Level 'INFO'
Write-Log -Message "Output CSV: $OutputPath" -Level 'INFO'
Write-Log -Message "Activity Log: $LogPath" -Level 'INFO'
Write-Log -Message "Error Log: $ErrorLogPath" -Level 'INFO'
Write-Log -Message "AddSelfToWorkspaces: $AddSelfToWorkspaces" -Level 'INFO'
Write-Log -Message "UseBulkExport: $UseBulkExport" -Level 'INFO'
if ($WorkspaceFilter) { Write-Log -Message "WorkspaceFilter: $WorkspaceFilter" -Level 'INFO' }

# Step 1: Authenticate
Write-Host "[1/7] Authenticating..." -ForegroundColor Yellow

# Ensure Az.Accounts module is available (required for Fabric API token acquisition)
if (-not (Get-Module -ListAvailable -Name Az.Accounts)) {
    Write-Host "  Installing Az.Accounts module (required for Fabric API authentication)..." -ForegroundColor Gray
    Install-Module -Name Az.Accounts -Scope CurrentUser -Force -AllowClobber
}
Import-Module Az.Accounts -ErrorAction Stop

try {
    # Check if already connected
    $azContext = Get-AzContext -ErrorAction Stop
    if (-not $azContext -or -not $azContext.Account) {
        Write-Host "  No Azure context found. Signing in..." -ForegroundColor Gray
        Connect-AzAccount -ErrorAction Stop | Out-Null
        $azContext = Get-AzContext
    }
    else {
        Write-Host "  Using existing Azure context: $($azContext.Account.Id)" -ForegroundColor Gray
    }
}
catch {
    Write-Host "  Connecting to Azure..." -ForegroundColor Gray
    Connect-AzAccount -ErrorAction Stop | Out-Null
    $azContext = Get-AzContext
}

$fabricToken = Get-FabricToken
$pbiToken = Get-PowerBIToken
$fabricHeaders = Get-AuthHeaders -Token $fabricToken
$pbiHeaders = Get-AuthHeaders -Token $pbiToken
$currentUserObjectId = Get-CurrentUserObjectId -Token $fabricToken
$currentUserEmail = Get-CurrentUserEmail -Token $fabricToken

Write-Host "  Authenticated as: $currentUserEmail (OID: $currentUserObjectId)" -ForegroundColor Green
Write-Log -Message "Authenticated as: $currentUserEmail (OID: $currentUserObjectId)" -Level 'INFO'
Write-Host ""

# Track token refresh time
$tokenRefreshTime = Get-Date

# Step 2: List all workspaces
Write-Host "[2/7] Listing all workspaces..." -ForegroundColor Yellow
$allWorkspaces = Get-AllWorkspaces -Headers $fabricHeaders

# Separate workspace types
$sharedWorkspaces = $allWorkspaces | Where-Object { $_.type -eq "Workspace" }
$personalWorkspaces = $allWorkspaces | Where-Object { $_.type -eq "Personal" }

if ($WorkspaceFilter) {
    $sharedWorkspaces = $sharedWorkspaces | Where-Object { $_.name -like $WorkspaceFilter }
    Write-Host "  Applied workspace filter: '$WorkspaceFilter'" -ForegroundColor Gray
}

Write-Host "  Found $($sharedWorkspaces.Count) shared workspaces, $($personalWorkspaces.Count) personal workspaces" -ForegroundColor Green
Write-Log -Message "Found $($sharedWorkspaces.Count) shared workspaces, $($personalWorkspaces.Count) personal workspaces" -Level 'INFO'
Write-Host ""

# Step 3: List all reports
Write-Host "[3/7] Listing all reports..." -ForegroundColor Yellow
$allReports = Get-AllReports -FabricHeaders $fabricHeaders -PbiHeaders $pbiHeaders

# Filter out reports that don't support getDefinition or can't contain custom visuals:
# - App reports (have appId set, or name starts with '[App]') - published app copies
# - Usage Metrics Reports - system-generated internal reports
$skipFilter = {
    $_.appId -or
    ($_.name -and $_.name.StartsWith('[App]')) -or
    ($_.name -eq 'Usage Metrics Report')
}
$skippedReports = $allReports | Where-Object $skipFilter
$allReports = $allReports | Where-Object { -not (& $skipFilter) }

$skippedCount = if ($skippedReports) { @($skippedReports).Count } else { 0 }
Write-Host "  Found $($allReports.Count) Power BI reports ($skippedCount skipped: app/system reports)" -ForegroundColor Green
Write-Log -Message "Found $($allReports.Count) Power BI reports, skipped $skippedCount (app/system reports)" -Level 'INFO'
if ($skippedReports) {
    foreach ($sr in $skippedReports) {
        Write-Log -Message "  Skipped report: '$($sr.name)' (Id: $($sr.id), Workspace: $($sr.workspaceId), AppId: $($sr.appId))" -Level 'DEBUG'
    }
}
Write-Host ""

# Build workspace lookup
$workspaceLookup = @{}
foreach ($ws in $allWorkspaces) {
    $workspaceLookup[$ws.id] = $ws
}

# Group reports by workspace
$reportsByWorkspace = @{}
foreach ($report in $allReports) {
    $wsId = $report.workspaceId
    if (-not $reportsByWorkspace.ContainsKey($wsId)) {
        $reportsByWorkspace[$wsId] = [System.Collections.Generic.List[object]]::new()
    }
    $reportsByWorkspace[$wsId].Add($report)
}

$stats.ReportsTotal = $allReports.Count

# Download AppSource custom visuals lookup
Write-Host "[4/8] Downloading AppSource custom visuals catalog..." -ForegroundColor Yellow
$appSourceLookup = @{}
try {
    $csvData = Invoke-RestMethod -Uri $AppSourceVisualsUrl -ErrorAction Stop
    $csvRecords = $csvData | ConvertFrom-Csv
    foreach ($row in $csvRecords) {
        $guid = $row.'Visual GUID'
        if ($guid) {
            $appSourceLookup[$guid] = @{
                Name         = $row.'Custom Visual'
                Publisher    = $row.'Publisher'
                Version      = $row.'Version'
                IsCertified  = $row.'Is Certified'
                AppSourceLink = $row.'AppSource Link'
            }
        }
    }
    Write-Host "  Loaded $($appSourceLookup.Count) AppSource visuals for lookup" -ForegroundColor Green
    Write-Log -Message "Loaded $($appSourceLookup.Count) AppSource visuals from DataChant catalog" -Level 'INFO'
}
catch {
    Write-Warning "Could not download AppSource visuals catalog: $_. Certified status will be 'Unknown'."
    Write-Log -Message "Failed to download AppSource visuals catalog: $_" -Level 'WARN'
}

# Step 5: Flag personal workspace reports
Write-Host "[5/8] Flagging personal workspace reports..." -ForegroundColor Yellow
$personalReportCount = 0
foreach ($ws in $personalWorkspaces) {
    if ($reportsByWorkspace.ContainsKey($ws.id)) {
        foreach ($report in $reportsByWorkspace[$ws.id]) {
            $personalReportCount++
            Write-CsvRow ([PSCustomObject]@{
                WorkspaceName           = $ws.name
                WorkspaceId             = $ws.id
                WorkspaceType           = "Personal"
                ReportName              = $report.name
                ReportId                = $report.id
                ReportUrl               = "https://app.fabric.microsoft.com/groups/$($ws.id)/reports/$($report.id)"
                ScanStatus              = "Skipped_PersonalWorkspace"
                CustomVisualId          = ""
                                    CustomVisualName        = ""
                CustomVisualDisplayName = ""
                CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                    IsCertified             = ""
                DefinitionFormat        = ""
            })
            $stats.ReportsSkipped++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Skipped" -Level 'WARN'
        }
    }
}
Write-Host "  Flagged $personalReportCount reports in personal workspaces" -ForegroundColor Green
Write-Host ""

# Step 5: Filter to shared workspaces that have reports
$workspacesToScan = $sharedWorkspaces | Where-Object { $reportsByWorkspace.ContainsKey($_.id) }
$stats.WorkspacesTotal = $workspacesToScan.Count

Write-Host "[6/8] Scanning $($workspacesToScan.Count) shared workspaces for custom visuals..." -ForegroundColor Yellow
Write-Log -Message "Starting scan of $($workspacesToScan.Count) shared workspaces containing reports" -Level 'INFO'
Write-Host ""

# Step 6: Process each workspace
$workspaceIndex = 0
$workspacesAddedSelf = [System.Collections.Generic.List[string]]::new()

foreach ($workspace in $workspacesToScan) {
    $workspaceIndex++
    $wsReports = $reportsByWorkspace[$workspace.id]
    $percentComplete = [Math]::Floor(($workspaceIndex / $workspacesToScan.Count) * 100)

    Write-Progress -Activity "Scanning workspaces" `
        -Status "[$workspaceIndex/$($workspacesToScan.Count)] '$($workspace.name)' ($($wsReports.Count) reports)" `
        -PercentComplete $percentComplete

    Write-Log -Message "--- Workspace [$workspaceIndex/$($workspacesToScan.Count)]: '$($workspace.name)' ($($workspace.id)) - $($wsReports.Count) reports ---" -Level 'INFO'

    # Refresh token if needed (every 40 minutes)
    if (((Get-Date) - $tokenRefreshTime).TotalMinutes -gt 40) {
        Write-Verbose "Refreshing access tokens..."
        try {
            $fabricToken = Get-FabricToken
            $pbiToken = Get-PowerBIToken
            $fabricHeaders = Get-AuthHeaders -Token $fabricToken
            $pbiHeaders = Get-AuthHeaders -Token $pbiToken
            $tokenRefreshTime = Get-Date
        }
        catch {
            Write-ErrorLog "Token refresh failed: $_"
        }
    }

    $addedSelfThisWorkspace = $false

    try {
        $definitions = $null
        $accessDenied = $false
        $bulkFailed = $false

        if ($UseBulkExport) {
            # --- Bulk Export approach ---
            try {
                $definitions = Get-BulkReportDefinitions -WorkspaceId $workspace.id -ReportIds @($wsReports.id) -Headers $fabricHeaders
            }
            catch {
                $sc = $null
                if ($_.Exception.Response) { $sc = [int]$_.Exception.Response.StatusCode }

                if ($sc -eq 403 -and $AddSelfToWorkspaces) {
                    Write-Verbose "  Access denied for workspace '$($workspace.name)'. Adding self as admin..."
                    $added = Add-SelfToWorkspace -WorkspaceId $workspace.id -WorkspaceName $workspace.name -UserEmail $currentUserEmail -Headers $pbiHeaders
                    if ($added) {
                        $addedSelfThisWorkspace = $true
                        $workspacesAddedSelf.Add($workspace.id)
                        Start-Sleep -Seconds 5  # Wait for permission propagation
                        # Retry
                        try {
                            $definitions = Get-BulkReportDefinitions -WorkspaceId $workspace.id -ReportIds @($wsReports.id) -Headers $fabricHeaders
                        }
                        catch {
                            Write-ErrorLog "Bulk export failed for workspace '$($workspace.name)' even after adding self: $_"
                            $accessDenied = $true
                        }
                    }
                    else {
                        $accessDenied = $true
                    }
                }
                elseif ($sc -eq 403) {
                    $accessDenied = $true
                    Write-ErrorLog "Access denied for workspace '$($workspace.name)'. Use -AddSelfToWorkspaces to auto-grant access."
                }
                else {
                    # Bulk export failed (UnknownError, transient issue, etc.) - fall back to individual getDefinition
                    Write-Warning "Bulk export failed for workspace '$($workspace.name)'. Falling back to individual report definitions..."
                    Write-Log -Message "Bulk export failed for workspace '$($workspace.name)': $_ - falling back to individual getDefinition" -Level 'WARN'
                    $bulkFailed = $true
                }
            }

            # --- Fallback: individual getDefinition when bulk export failed ---
            if ($bulkFailed) {
                $reportIndex = 0
                foreach ($report in $wsReports) {
                    $reportIndex++
                    Write-Progress -Id 1 -Activity "Fallback: individual definitions in '$($workspace.name)'" `
                        -Status "[$reportIndex/$($wsReports.Count)] '$($report.name)'" `
                        -PercentComplete ([Math]::Floor(($reportIndex / $wsReports.Count) * 100))
                    try {
                        $definition = Get-ReportDefinition -WorkspaceId $workspace.id -ReportId $report.id -Headers $fabricHeaders
                        $parsed = Extract-CustomVisualsFromDefinition -Definition $definition -ReportId $report.id -AppSourceLookup $appSourceLookup

                        if ($parsed.Visuals.Count -gt 0) {
                            foreach ($cv in $parsed.Visuals) {
                                Write-CsvRow ([PSCustomObject]@{
                                    WorkspaceName           = $workspace.name
                                    WorkspaceId             = $workspace.id
                                    WorkspaceType           = "Workspace"
                                    ReportName              = $report.name
                                    ReportId                = $report.id
                                    ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                                    ScanStatus              = "Success"
                                    CustomVisualId          = $cv.CustomVisualId
                                    CustomVisualName        = $cv.CustomVisualName
                                    CustomVisualDisplayName = $cv.CustomVisualDisplayName
                                    CustomVisualVersion     = $cv.CustomVisualVersion
                                    CustomVisualPublisher   = $cv.CustomVisualPublisher
                                    CustomVisualSource      = $cv.CustomVisualSource
                                    AppSourceLink           = $cv.AppSourceLink
                                    PageName                = $cv.PageName
                                    IsCertified             = $cv.IsCertified
                                    DefinitionFormat        = $parsed.Format
                                })
                                $stats.CustomVisualsFound++
                            }
                        }
                        else {
                            Write-CsvRow ([PSCustomObject]@{
                                WorkspaceName           = $workspace.name
                                WorkspaceId             = $workspace.id
                                WorkspaceType           = "Workspace"
                                ReportName              = $report.name
                                ReportId                = $report.id
                                ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                                ScanStatus              = "Success_NoCustomVisuals"
                                CustomVisualId          = ""
                                CustomVisualName        = ""
                                CustomVisualDisplayName = ""
                                CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                IsCertified             = ""
                                DefinitionFormat        = $parsed.Format
                            })
                        }
                        $stats.ReportsScanned++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Scanned successfully (fallback), $($parsed.Visuals.Count) custom visual(s) found" -Level 'INFO'
                    }
                    catch [System.IO.FileNotFoundException] {
                        # App reports, unsupported items - log as skipped
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Skipped (app report or unsupported item)" -Level 'WARN'
                        Write-CsvRow ([PSCustomObject]@{
                            WorkspaceName           = $workspace.name
                            WorkspaceId             = $workspace.id
                            WorkspaceType           = "Workspace"
                            ReportName              = $report.name
                            ReportId                = $report.id
                            ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                            ScanStatus              = "Skipped_UnsupportedItem"
                            CustomVisualId          = ""
                            CustomVisualName        = ""
                            CustomVisualDisplayName = ""
                            CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                            IsCertified             = ""
                            DefinitionFormat        = ""
                        })
                        $stats.ReportsSkipped++
                    }
                    catch {
                        Write-ErrorLog "Error getting definition for report '$($report.name)' in '$($workspace.name)': $_"
                        Write-CsvRow ([PSCustomObject]@{
                            WorkspaceName           = $workspace.name
                            WorkspaceId             = $workspace.id
                            WorkspaceType           = "Workspace"
                            ReportName              = $report.name
                            ReportId                = $report.id
                            ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                            ScanStatus              = "Error"
                            CustomVisualId          = ""
                            CustomVisualName        = ""
                            CustomVisualDisplayName = ""
                            CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                            IsCertified             = ""
                            DefinitionFormat        = ""
                        })
                        $stats.ReportsErrored++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Error during processing" -Level 'ERROR'
                    }
                }
                Write-Progress -Id 1 -Activity "Fallback" -Completed
                $stats.WorkspacesScanned++
                Write-Log -Message "Workspace '$($workspace.name)' scan complete (via fallback)" -Level 'INFO'
            }

            # Parse bulk export results
            if ($definitions -and -not $accessDenied -and -not $bulkFailed) {
                # Unwrap if the response is nested (e.g., LRO result wrapping)
                $defData = $definitions
                if (-not $defData.itemDefinitionsIndex -and $defData.definition) {
                    $defData = $defData.definition
                }
                if (-not $defData.itemDefinitionsIndex -and $defData.result) {
                    $defData = $defData.result
                }

                # Log the raw structure for debugging
                $indexCount = if ($defData.itemDefinitionsIndex) { @($defData.itemDefinitionsIndex).Count } else { 0 }
                $partsCount = if ($defData.definitionParts) { @($defData.definitionParts).Count } else { 0 }
                Write-Log -Message "  Bulk export returned: $indexCount items in index, $partsCount definition parts" -Level 'DEBUG'
                if ($indexCount -eq 0) {
                    # Log available property names to help debug
                    $propNames = ($defData | Get-Member -MemberType NoteProperty | ForEach-Object { $_.Name }) -join ', '
                    Write-Log -Message "  Bulk export response properties: $propNames" -Level 'DEBUG'
                }
                if ($defData.itemDefinitionsIndex) {
                    foreach ($idx in $defData.itemDefinitionsIndex) {
                        Write-Log -Message "    Index entry: id=$($idx.id), rootPath=$($idx.rootPath)" -Level 'DEBUG'
                    }
                }

                # Build index mapping from rootPath to itemId
                $itemIndex = @{}
                if ($defData.itemDefinitionsIndex) {
                    foreach ($idx in $defData.itemDefinitionsIndex) {
                        $itemIndex[$idx.rootPath] = $idx.id
                    }
                }

                # Group definition parts by report
                $partsByReport = @{}
                if ($defData.definitionParts) {
                    foreach ($part in $defData.definitionParts) {
                        # Find which report this part belongs to by matching path prefix
                        $matchedReportId = $null
                        foreach ($rootEntry in $defData.itemDefinitionsIndex) {
                            if ($part.path.StartsWith($rootEntry.rootPath)) {
                                $matchedReportId = $rootEntry.id
                                break
                            }
                        }
                        if (-not $matchedReportId) {
                            Write-Log -Message "    Unmatched part path: $($part.path)" -Level 'DEBUG'
                        }
                        if ($matchedReportId) {
                            if (-not $partsByReport.ContainsKey($matchedReportId)) {
                                $partsByReport[$matchedReportId] = [System.Collections.Generic.List[object]]::new()
                            }
                            $partsByReport[$matchedReportId].Add($part)
                        }
                    }
                }

                Write-Log -Message "  Matched parts to $($partsByReport.Keys.Count) reports. Report IDs in partsByReport: $($partsByReport.Keys -join ', ')" -Level 'DEBUG'
                Write-Log -Message "  Report IDs from wsReports: $(($wsReports | ForEach-Object { $_.id }) -join ', ')" -Level 'DEBUG'

                foreach ($report in $wsReports) {
                    if ($partsByReport.ContainsKey($report.id)) {
                        $defObj = @{ definitionParts = $partsByReport[$report.id] }
                        $parsed = Extract-CustomVisualsFromDefinition -Definition $defObj -ReportId $report.id -AppSourceLookup $appSourceLookup

                        if ($parsed.Visuals.Count -gt 0) {
                            foreach ($cv in $parsed.Visuals) {
                                Write-CsvRow ([PSCustomObject]@{
                                    WorkspaceName           = $workspace.name
                                    WorkspaceId             = $workspace.id
                                    WorkspaceType           = "Workspace"
                                    ReportName              = $report.name
                                    ReportId                = $report.id
                                    ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                                    ScanStatus              = "Success"
                                    CustomVisualId          = $cv.CustomVisualId
                                    CustomVisualName        = $cv.CustomVisualName
                                    CustomVisualDisplayName = $cv.CustomVisualDisplayName
                                    CustomVisualVersion     = $cv.CustomVisualVersion
                                    CustomVisualPublisher   = $cv.CustomVisualPublisher
                                    CustomVisualSource      = $cv.CustomVisualSource
                                    AppSourceLink           = $cv.AppSourceLink
                                    PageName                = $cv.PageName
                                    IsCertified             = $cv.IsCertified
                                    DefinitionFormat        = $parsed.Format
                                })
                                $stats.CustomVisualsFound++
                            }
                        }
                        else {
                            # Report scanned but no custom visuals found
                            Write-CsvRow ([PSCustomObject]@{
                                WorkspaceName           = $workspace.name
                                WorkspaceId             = $workspace.id
                                WorkspaceType           = "Workspace"
                                ReportName              = $report.name
                                ReportId                = $report.id
                                ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                                ScanStatus              = "Success_NoCustomVisuals"
                                CustomVisualId          = ""
                                    CustomVisualName        = ""
                                CustomVisualDisplayName = ""
                                CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                    IsCertified             = ""
                                DefinitionFormat        = $parsed.Format
                            })
                        }
                        $stats.ReportsScanned++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Scanned successfully, $($parsed.Visuals.Count) custom visual(s) found" -Level 'INFO'
                    }
                    else {
                        # Report not in bulk export (might not be a supported type)
                        Write-CsvRow ([PSCustomObject]@{
                            WorkspaceName           = $workspace.name
                            WorkspaceId             = $workspace.id
                            WorkspaceType           = "Workspace"
                            ReportName              = $report.name
                            ReportId                = $report.id
                            ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                            ScanStatus              = "NotInBulkExport"
                            CustomVisualId          = ""
                                    CustomVisualName        = ""
                            CustomVisualDisplayName = ""
                            CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                    IsCertified             = ""
                            DefinitionFormat        = ""
                        })
                        $stats.ReportsSkipped++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Skipped" -Level 'WARN'
                    }
                }
                $stats.WorkspacesScanned++
                Write-Log -Message "Workspace '$($workspace.name)' scan complete" -Level 'INFO'
            }
            elseif ($accessDenied) {
                foreach ($report in $wsReports) {
                    Write-CsvRow ([PSCustomObject]@{
                        WorkspaceName           = $workspace.name
                        WorkspaceId             = $workspace.id
                        WorkspaceType           = "Workspace"
                        ReportName              = $report.name
                        ReportId                = $report.id
                        ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                        ScanStatus              = "AccessDenied"
                        CustomVisualId          = ""
                                    CustomVisualName        = ""
                        CustomVisualDisplayName = ""
                        CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                    IsCertified             = ""
                        DefinitionFormat        = ""
                    })
                    $stats.ReportsSkipped++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Skipped" -Level 'WARN'
                }
                $stats.WorkspacesAccessErr++
                Write-Log -Message "Workspace '$($workspace.name)' - access denied" -Level 'WARN'
            }
        }
        else {
            # --- Individual Get Definition approach ---
            $reportIndex = 0
            $wsAccessDenied = $false

            foreach ($report in $wsReports) {
                $reportIndex++
                Write-Progress -Id 1 -Activity "Processing reports in '$($workspace.name)'" `
                    -Status "[$reportIndex/$($wsReports.Count)] '$($report.name)'" `
                    -PercentComplete ([Math]::Floor(($reportIndex / $wsReports.Count) * 100))

                if ($wsAccessDenied) {
                    Write-CsvRow ([PSCustomObject]@{
                        WorkspaceName           = $workspace.name
                        WorkspaceId             = $workspace.id
                        WorkspaceType           = "Workspace"
                        ReportName              = $report.name
                        ReportId                = $report.id
                        ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                        ScanStatus              = "AccessDenied"
                        CustomVisualId          = ""
                                    CustomVisualName        = ""
                        CustomVisualDisplayName = ""
                        CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                    IsCertified             = ""
                        DefinitionFormat        = ""
                    })
                    $stats.ReportsSkipped++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Skipped" -Level 'WARN'
                    continue
                }

                try {
                    $definition = Get-ReportDefinition -WorkspaceId $workspace.id -ReportId $report.id -Headers $fabricHeaders
                    $parsed = Extract-CustomVisualsFromDefinition -Definition $definition -ReportId $report.id -AppSourceLookup $appSourceLookup

                    if ($parsed.Visuals.Count -gt 0) {
                        foreach ($cv in $parsed.Visuals) {
                            Write-CsvRow ([PSCustomObject]@{
                                WorkspaceName           = $workspace.name
                                WorkspaceId             = $workspace.id
                                WorkspaceType           = "Workspace"
                                ReportName              = $report.name
                                ReportId                = $report.id
                                ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                                ScanStatus              = "Success"
                                CustomVisualId          = $cv.CustomVisualId
                                    CustomVisualName        = $cv.CustomVisualName
                                CustomVisualDisplayName = $cv.CustomVisualDisplayName
                                CustomVisualVersion     = $cv.CustomVisualVersion
                                    CustomVisualPublisher   = $cv.CustomVisualPublisher
                                    CustomVisualSource      = $cv.CustomVisualSource
                                    AppSourceLink           = $cv.AppSourceLink
                                    PageName                = $cv.PageName
                                    IsCertified             = $cv.IsCertified
                                DefinitionFormat        = $parsed.Format
                            })
                            $stats.CustomVisualsFound++
                        }
                    }
                    else {
                        Write-CsvRow ([PSCustomObject]@{
                            WorkspaceName           = $workspace.name
                            WorkspaceId             = $workspace.id
                            WorkspaceType           = "Workspace"
                            ReportName              = $report.name
                            ReportId                = $report.id
                            ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                            ScanStatus              = "Success_NoCustomVisuals"
                            CustomVisualId          = ""
                                    CustomVisualName        = ""
                            CustomVisualDisplayName = ""
                            CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                    IsCertified             = ""
                            DefinitionFormat        = $parsed.Format
                        })
                    }
                    $stats.ReportsScanned++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Scanned successfully, $($parsed.Visuals.Count) custom visual(s) found" -Level 'INFO'
                }
                catch [System.IO.FileNotFoundException] {
                    Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Skipped (app report or unsupported item)" -Level 'WARN'
                    Write-CsvRow ([PSCustomObject]@{
                        WorkspaceName           = $workspace.name
                        WorkspaceId             = $workspace.id
                        WorkspaceType           = "Workspace"
                        ReportName              = $report.name
                        ReportId                = $report.id
                        ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                        ScanStatus              = "Skipped_UnsupportedItem"
                        CustomVisualId          = ""
                        CustomVisualName        = ""
                        CustomVisualDisplayName = ""
                        CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                        IsCertified             = ""
                        DefinitionFormat        = ""
                    })
                    $stats.ReportsSkipped++
                }
                catch {
                    $sc = $null
                    if ($_.Exception.Response) { $sc = [int]$_.Exception.Response.StatusCode }

                    if ($sc -eq 403 -and $AddSelfToWorkspaces -and -not $addedSelfThisWorkspace) {
                        Write-Verbose "  Access denied. Adding self as admin to '$($workspace.name)'..."
                        $added = Add-SelfToWorkspace -WorkspaceId $workspace.id -WorkspaceName $workspace.name -UserEmail $currentUserEmail -Headers $pbiHeaders
                        if ($added) {
                            $addedSelfThisWorkspace = $true
                            $workspacesAddedSelf.Add($workspace.id)
                            Start-Sleep -Seconds 5
                            # Retry this report
                            try {
                                $definition = Get-ReportDefinition -WorkspaceId $workspace.id -ReportId $report.id -Headers $fabricHeaders
                                $parsed = Extract-CustomVisualsFromDefinition -Definition $definition -ReportId $report.id -AppSourceLookup $appSourceLookup

                                if ($parsed.Visuals.Count -gt 0) {
                                    foreach ($cv in $parsed.Visuals) {
                                        Write-CsvRow ([PSCustomObject]@{
                                            WorkspaceName           = $workspace.name
                                            WorkspaceId             = $workspace.id
                                            WorkspaceType           = "Workspace"
                                            ReportName              = $report.name
                                            ReportId                = $report.id
                                            ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                                            ScanStatus              = "Success"
                                            CustomVisualId          = $cv.CustomVisualId
                                    CustomVisualName        = $cv.CustomVisualName
                                            CustomVisualDisplayName = $cv.CustomVisualDisplayName
                                            CustomVisualVersion     = $cv.CustomVisualVersion
                                    CustomVisualPublisher   = $cv.CustomVisualPublisher
                                    CustomVisualSource      = $cv.CustomVisualSource
                                    AppSourceLink           = $cv.AppSourceLink
                                    PageName                = $cv.PageName
                                    IsCertified             = $cv.IsCertified
                                            DefinitionFormat        = $parsed.Format
                                        })
                                        $stats.CustomVisualsFound++
                                    }
                                }
                                else {
                                    Write-CsvRow ([PSCustomObject]@{
                                        WorkspaceName           = $workspace.name
                                        WorkspaceId             = $workspace.id
                                        WorkspaceType           = "Workspace"
                                        ReportName              = $report.name
                                        ReportId                = $report.id
                                        ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                                        ScanStatus              = "Success_NoCustomVisuals"
                                        CustomVisualId          = ""
                                    CustomVisualName        = ""
                                        CustomVisualDisplayName = ""
                                        CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                    IsCertified             = ""
                                        DefinitionFormat        = $parsed.Format
                                    })
                                }
                                $stats.ReportsScanned++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Scanned successfully, $($parsed.Visuals.Count) custom visual(s) found" -Level 'INFO'
                            }
                            catch [System.IO.FileNotFoundException] {
                                Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Skipped (app report or unsupported item)" -Level 'WARN'
                                Write-CsvRow ([PSCustomObject]@{
                                    WorkspaceName           = $workspace.name
                                    WorkspaceId             = $workspace.id
                                    WorkspaceType           = "Workspace"
                                    ReportName              = $report.name
                                    ReportId                = $report.id
                                    ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                                    ScanStatus              = "Skipped_UnsupportedItem"
                                    CustomVisualId          = ""
                                    CustomVisualName        = ""
                                    CustomVisualDisplayName = ""
                                    CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                    IsCertified             = ""
                                    DefinitionFormat        = ""
                                })
                                $stats.ReportsSkipped++
                            }
                            catch {
                                Write-ErrorLog "Failed to get definition for report '$($report.name)' even after adding self: $_"
                                Write-CsvRow ([PSCustomObject]@{
                                    WorkspaceName           = $workspace.name
                                    WorkspaceId             = $workspace.id
                                    WorkspaceType           = "Workspace"
                                    ReportName              = $report.name
                                    ReportId                = $report.id
                                    ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                                    ScanStatus              = "Error"
                                    CustomVisualId          = ""
                                    CustomVisualName        = ""
                                    CustomVisualDisplayName = ""
                                    CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                    IsCertified             = ""
                                    DefinitionFormat        = ""
                                })
                                $stats.ReportsErrored++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Error during processing" -Level 'ERROR'
                            }
                        }
                        else {
                            $wsAccessDenied = $true
                            Write-CsvRow ([PSCustomObject]@{
                                WorkspaceName           = $workspace.name
                                WorkspaceId             = $workspace.id
                                WorkspaceType           = "Workspace"
                                ReportName              = $report.name
                                ReportId                = $report.id
                                ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                                ScanStatus              = "AccessDenied"
                                CustomVisualId          = ""
                                    CustomVisualName        = ""
                                CustomVisualDisplayName = ""
                                CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                    IsCertified             = ""
                                DefinitionFormat        = ""
                            })
                            $stats.ReportsSkipped++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Skipped" -Level 'WARN'
                        }
                    }
                    elseif ($sc -eq 403) {
                        $wsAccessDenied = $true
                        Write-ErrorLog "Access denied for workspace '$($workspace.name)'. Use -AddSelfToWorkspaces to auto-grant."
                        Write-CsvRow ([PSCustomObject]@{
                            WorkspaceName           = $workspace.name
                            WorkspaceId             = $workspace.id
                            WorkspaceType           = "Workspace"
                            ReportName              = $report.name
                            ReportId                = $report.id
                            ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                            ScanStatus              = "AccessDenied"
                            CustomVisualId          = ""
                                    CustomVisualName        = ""
                            CustomVisualDisplayName = ""
                            CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                    IsCertified             = ""
                            DefinitionFormat        = ""
                        })
                        $stats.ReportsSkipped++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Skipped" -Level 'WARN'
                    }
                    else {
                        Write-ErrorLog "Error getting definition for report '$($report.name)' in '$($workspace.name)': $_"
                        Write-CsvRow ([PSCustomObject]@{
                            WorkspaceName           = $workspace.name
                            WorkspaceId             = $workspace.id
                            WorkspaceType           = "Workspace"
                            ReportName              = $report.name
                            ReportId                = $report.id
                            ReportUrl               = "https://app.fabric.microsoft.com/groups/$($workspace.id)/reports/$($report.id)"
                            ScanStatus              = "Error"
                            CustomVisualId          = ""
                                    CustomVisualName        = ""
                            CustomVisualDisplayName = ""
                            CustomVisualVersion     = ""
                                    CustomVisualPublisher   = ""
                                    CustomVisualSource      = ""
                                    AppSourceLink           = ""
                                    PageName                = ""
                                    IsCertified             = ""
                            DefinitionFormat        = ""
                        })
                        $stats.ReportsErrored++
                        Write-Log -Message "  Report: '$($report.name)' ($($report.id)) - Error during processing" -Level 'ERROR'
                    }
                }
            }

            if (-not $wsAccessDenied) {
                $stats.WorkspacesScanned++
                Write-Log -Message "Workspace '$($workspace.name)' scan complete" -Level 'INFO'
            }
            else {
                $stats.WorkspacesAccessErr++
                Write-Log -Message "Workspace '$($workspace.name)' - access denied" -Level 'WARN'
            }

            Write-Progress -Id 1 -Activity "Processing reports" -Completed
        }
    }
    finally {
        # Cleanup: remove self from workspace if added
        if ($addedSelfThisWorkspace) {
            Write-Verbose "  Removing self from workspace '$($workspace.name)'..."
            Remove-SelfFromWorkspace -WorkspaceId $workspace.id -WorkspaceName $workspace.name -UserEmail $currentUserEmail -Headers $pbiHeaders
        }
    }
}

Write-Progress -Activity "Scanning workspaces" -Completed

# Step 7: Export results
Write-Host ""
Write-Host "[7/8] Exporting results..." -ForegroundColor Yellow

# CSV already written incrementally
Write-Host "  CSV saved to: $OutputPath" -ForegroundColor Green

# Step 8: Summary
Write-Host ""
Write-Host "[8/8] Scan Complete!" -ForegroundColor Yellow
Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  SUMMARY" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  Workspaces found:         $($stats.WorkspacesTotal) shared + $($personalWorkspaces.Count) personal" -ForegroundColor White
Write-Host "  Workspaces scanned:       $($stats.WorkspacesScanned)" -ForegroundColor Green
Write-Host "  Workspaces access denied: $($stats.WorkspacesAccessErr)" -ForegroundColor $(if ($stats.WorkspacesAccessErr -gt 0) { "Yellow" } else { "Green" })
Write-Host "  Reports total:            $($stats.ReportsTotal)" -ForegroundColor White
Write-Host "  Reports scanned:          $($stats.ReportsScanned)" -ForegroundColor Green
Write-Host "  Reports skipped:          $($stats.ReportsSkipped) (personal/access denied)" -ForegroundColor $(if ($stats.ReportsSkipped -gt 0) { "Yellow" } else { "Green" })
Write-Host "  Reports errored:          $($stats.ReportsErrored)" -ForegroundColor $(if ($stats.ReportsErrored -gt 0) { "Red" } else { "Green" })
Write-Host "  Custom visuals found:     $($stats.CustomVisualsFound)" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Output: $OutputPath" -ForegroundColor White
if (Test-Path $ErrorLogPath) {
    Write-Host "  Errors: $ErrorLogPath" -ForegroundColor Yellow
}
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

#endregion
