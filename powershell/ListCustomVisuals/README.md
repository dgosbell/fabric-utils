# Get-CustomVisuals

A PowerShell script that scans all Power BI reports across a Microsoft Fabric tenant to identify custom visuals. It produces a CSV report detailing which custom visuals are used in each report, on which page, and whether they are certified.

## Features

- **Tenant-wide scanning** — discovers all workspaces and reports via the Fabric Admin APIs
- **Dual format support** — parses both PBIR (enhanced) and PBIR-Legacy report definitions
- **Page-level detail** — identifies which page each custom visual instance appears on
- **Workspace access management** — optionally adds/removes the executing admin to workspaces temporarily
- **Personal workspace awareness** — flags reports in personal workspaces (cannot be scanned, but are listed)
- **Bulk export support** — uses the Bulk Export Item Definitions (beta) API for faster scanning
- **Comprehensive logging** — activity log tracks every workspace/report processed and all access changes
- **Progress indicators** — shows real-time progress via `Write-Progress`
- **Rate limit handling** — automatic retry with backoff for throttled requests
- **Token refresh** — automatically refreshes authentication tokens during long-running scans

## Prerequisites

1. **PowerShell 5.1+** (PowerShell 7+ recommended for performance)
2. **Az.Accounts module** — used for authentication (auto-installed on first run if missing)
   ```powershell
   Install-Module Az.Accounts -Scope CurrentUser
   ```
   > **Why Az.Accounts?** The Fabric REST APIs (`api.fabric.microsoft.com`) require a token with audience `https://api.fabric.microsoft.com`. The `MicrosoftPowerBIMgmt` module's `Get-PowerBIAccessToken` returns tokens scoped to `https://analysis.windows.net/powerbi/api` which only works for Power BI APIs — not for Fabric APIs like `getDefinition` or `bulkExportDefinitions`. `Az.Accounts` can acquire tokens for any resource audience via `Get-AzAccessToken -ResourceUrl`. This is the same approach used by the [dataplat/FabricTools](https://github.com/dataplat/FabricTools) project.
3. **Fabric Administrator role** — the executing identity must be a Power BI Service Administrator, Fabric Administrator, or Global Administrator
4. **Tenant setting** — if using `-AddSelfToWorkspaces`, the tenant setting "Service admins can access workspaces" should be enabled

## Limitations
* Cannot scan reports iņ other users "My Workspace" (personal workspaces)
* Cannot scan the report instances in workspace Apps. It will scan the report in the source workspace, but if changes have not been published to the app there may be discrepencies.

## Authentication

Before running the scanner, authenticate to Azure:

```powershell
Connect-AzAccount
```

The script will use your existing Azure context. If no context is found, it will prompt you to sign in. The `Az.Accounts` module is auto-installed if missing.

## Usage

### Basic scan (individual report definition calls)

```powershell
.\Get-CustomVisuals.ps1
```

Scans all shared workspaces, retrieves each report definition individually, and outputs a CSV in the current directory.

### Bulk export mode (recommended for large tenants)

```powershell
.\Get-CustomVisuals.ps1 -UseBulkExport
```

Uses the Bulk Export Item Definitions (beta) API to download all report definitions per workspace in a single call. Significantly faster for workspaces with many reports.

### Auto-grant workspace access

```powershell
.\Get-CustomVisuals.ps1 -UseBulkExport -AddSelfToWorkspaces
```

If the executing admin doesn't have access to a workspace, the script will:
1. Add the admin as a workspace Admin via the Power BI Admin API
2. Download the report definitions
3. Remove the admin from the workspace

All access grants/revocations are logged in the activity log.

### Filter to specific workspaces

```powershell
.\Get-CustomVisuals.ps1 -WorkspaceFilter "Sales*"
```

Only scans workspaces whose names match the wildcard pattern.

### Custom output paths

```powershell
.\Get-CustomVisuals.ps1 -OutputPath "C:\Reports\visuals.csv" `
                        -LogPath "C:\Reports\activity.log" `
                        -ErrorLogPath "C:\Reports\errors.log"
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `-OutputPath` | String | Path for the output CSV. Default: `CustomVisuals_<timestamp>.csv` in current directory |
| `-LogPath` | String | Path for the activity log. Default: `CustomVisuals_Log_<timestamp>.log` in current directory |
| `-ErrorLogPath` | String | Path for the error log. Default: `CustomVisuals_Errors_<timestamp>.log` in current directory |
| `-AddSelfToWorkspaces` | Switch | Temporarily add/remove executing user as Admin to workspaces for access |
| `-WorkspaceFilter` | String | Filter workspaces by name (supports `*` and `?` wildcards) |
| `-UseBulkExport` | Switch | Use the Bulk Export Item Definitions (beta) API instead of individual calls |

## Output

### CSV Columns

| Column | Description |
|--------|-------------|
| `WorkspaceName` | Name of the workspace |
| `WorkspaceId` | GUID of the workspace |
| `WorkspaceType` | `Workspace` or `Personal` |
| `ReportName` | Name of the Power BI report |
| `ReportId` | GUID of the report |
| `ReportUrl` | Direct URL to the report in the Fabric portal |
| `ScanStatus` | `Success`, `Success_NoCustomVisuals`, `Skipped_PersonalWorkspace`, `AccessDenied`, `Error`, `NotInBulkExport` |
| `CustomVisualId` | Internal identifier / GUID of the custom visual |
| `CustomVisualName` | Technical name of the custom visual |
| `CustomVisualDisplayName` | Human-readable display name (from AppSource catalog if available) |
| `CustomVisualVersion` | Version string of the custom visual |
| `CustomVisualPublisher` | Publisher name (from AppSource catalog) |
| `CustomVisualSource` | `AppSource` or `Private` |
| `AppSourceLink` | Link to the visual in AppSource (if available) |
| `PageName` | Report page where the custom visual is used |
| `IsCertified` | `Yes`, `No`, `N/A` (private), or `Unknown` |
| `DefinitionFormat` | `PBIR` or `PBIR-Legacy` |

### Activity Log

The activity log (`CustomVisuals_Log_<timestamp>.log`) records:

- Script start/configuration
- Authentication details
- Each workspace being processed (name, ID, report count)
- Each report processed (name, ID, scan result, custom visual count)
- **All workspace access grants and revocations** (with timestamps)
- Workspace scan completion
- Errors and warnings

Example log entries:
```
[2026-03-20 10:15:23] [INFO] ========== Custom Visual Scanner Started ==========
[2026-03-20 10:15:23] [INFO] Authenticated as: admin@contoso.com (OID: abc-123)
[2026-03-20 10:15:25] [INFO] Found 47 shared workspaces, 120 personal workspaces
[2026-03-20 10:15:30] [INFO] --- Workspace [1/47]: 'Sales Analytics' (guid-1) - 12 reports ---
[2026-03-20 10:15:31] [ACCESS] ACCESS GRANTED: Added 'admin@contoso.com' as Admin to workspace 'Sales Analytics' (guid-1)
[2026-03-20 10:15:36] [INFO]   Report: 'Monthly Sales' (guid-2) - Scanned successfully, 3 custom visual(s) found
[2026-03-20 10:15:37] [INFO]   Report: 'Dashboard' (guid-3) - Scanned successfully, 0 custom visual(s) found
[2026-03-20 10:15:38] [ACCESS] ACCESS REVOKED: Removed 'admin@contoso.com' from workspace 'Sales Analytics' (guid-1)
[2026-03-20 10:15:38] [INFO] Workspace 'Sales Analytics' scan complete
```

## How It Works

### API Flow

```
1. Authenticate (Az.Accounts / Connect-AzAccount)
         │
2. GET /v1/admin/workspaces          ── List all tenant workspaces
         │
3. GET /v1/admin/items?type=Report   ── List all Power BI reports
         │
4. For each workspace with reports:
    ├── POST /v1/workspaces/{id}/items/bulkExportDefinitions  (bulk mode)
    │   OR
    ├── POST /v1/workspaces/{id}/items/{id}/getDefinition     (individual mode)
    │
    ├── If 403 & -AddSelfToWorkspaces:
    │   ├── POST /admin/groups/{id}/users    ── Add self as Admin
    │   ├── Retry definition export
    │   └── DELETE /admin/groups/{id}/users   ── Remove self
    │
5. Parse definitions (PBIR + PBIR-Legacy) → extract custom visuals
         │
6. Export CSV + Activity Log
```

### Custom Visual Detection

**PBIR-Legacy format** (single `report.json`):
- The `extensions` array lists registered custom visuals with name, version, and source
- Each `section` (page) contains `visualContainers` with a `visualType` property
- If `visualType` is `"extension"` or not in the built-in visual type list, it's a custom visual

**PBIR format** (folder-based):
- Each visual has its own `visual.json` at `definition/pages/{pageId}/visuals/{visualId}/visual.json`
- Page names are read from `page.json` files at `definition/pages/{pageId}/page.json`
- Custom visuals have `visualType: "extension"` or a non-built-in type name

### Personal Workspaces

Personal workspaces ("My Workspace") are discovered via the Admin API but **cannot be scanned** for report definitions. Tenant admins cannot add themselves to other users' personal workspaces. These reports appear in the CSV with `ScanStatus = "Skipped_PersonalWorkspace"`.

### Certified Visuals

Certification status is resolved automatically by downloading the [DataChant PowerBI-Visuals-AppSource](https://github.com/DataChant/PowerBI-Visuals-AppSource) catalog at startup. This provides display names, publishers, versions, AppSource links, and certification status for all AppSource visuals. Private/organizational visuals show `IsCertified = "N/A"`.

## Rate Limits

| API | Limit |
|-----|-------|
| Admin List Workspaces | 200 requests/hour |
| Admin List Items | 200 requests/hour |
| Bulk Export Definitions | Standard Fabric throttling |
| Get Item Definition | Standard Fabric throttling |

The module automatically handles 429 (Too Many Requests) responses with retry logic and respects `Retry-After` headers.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Failed to obtain Fabric API access token` | Run `Connect-AzAccount` first |
| `InsufficientPrivileges` on admin APIs | Ensure you are a Fabric Administrator |
| Many `AccessDenied` reports | Use `-AddSelfToWorkspaces` switch |
| Bulk export returns 404 | The beta API may not be available; remove `-UseBulkExport` |
| Script runs slowly | Use `-UseBulkExport` for batch processing; use `-WorkspaceFilter` to limit scope |
| Token expiry during long runs | The module auto-refreshes tokens every 40 minutes |

## License

Internal use — Microsoft FabricCAT.
