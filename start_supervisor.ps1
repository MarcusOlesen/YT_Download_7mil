[CmdletBinding()]
param(
    [string]$ConfigPath = "",
    [switch]$ValidateOnly,
    [switch]$EditConfig
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not [string]::IsNullOrWhiteSpace($PSScriptRoot)) {
    $repoRoot = $PSScriptRoot
}
elseif (-not [string]::IsNullOrWhiteSpace($MyInvocation.MyCommand.Path)) {
    $repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}
else {
    $repoRoot = (Get-Location).Path
}

$repoRoot = [System.IO.Path]::GetFullPath($repoRoot)
$supervisorPath = Join-Path $repoRoot "supervise_workers.ps1"
$exampleConfigPath = Join-Path $repoRoot "supervise_workers.config.example.psd1"

if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
    $ConfigPath = Join-Path $repoRoot "supervise_workers.config.psd1"
}
$ConfigPath = [System.IO.Path]::GetFullPath($ConfigPath)

if (-not (Test-Path -LiteralPath $supervisorPath)) {
    throw "Missing supervisor script: $supervisorPath"
}

if (-not (Test-Path -LiteralPath $ConfigPath)) {
    if (-not (Test-Path -LiteralPath $exampleConfigPath)) {
        throw "Missing config file and example template: $ConfigPath"
    }

    Copy-Item -LiteralPath $exampleConfigPath -Destination $ConfigPath -Force
    Write-Host "Created config file: $ConfigPath"
    Write-Host "Edit it and run start_supervisor.bat again."
    Write-Host "You can also run: .\\start_supervisor.bat -EditConfig"
    exit 1
}

if ($EditConfig) {
    Start-Process -FilePath "notepad.exe" -ArgumentList $ConfigPath
    return
}

$config = Import-PowerShellDataFile -Path $ConfigPath
if (-not ($config -is [hashtable])) {
    throw "Config file must evaluate to a hashtable: $ConfigPath"
}

$params = @{}
foreach ($entry in $config.GetEnumerator()) {
    $params[$entry.Key] = $entry.Value
}

if (
    (-not $params.ContainsKey("RepoRoot")) -or
    [string]::IsNullOrWhiteSpace([string]$params["RepoRoot"])
) {
    $params["RepoRoot"] = $repoRoot
}

if ($ValidateOnly) {
    $params["ValidateOnly"] = $true
}

& $supervisorPath @params
