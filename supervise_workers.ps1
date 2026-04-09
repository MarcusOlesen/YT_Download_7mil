<#
.SYNOPSIS
Supervise the SSH tunnel and selected long-running worker scripts.

.DESCRIPTION
Starts the SSH tunnel and any enabled worker processes, watches for exits,
and restarts them after a configurable delay. `backup_and_reap.py` is
optional and is started only when -StartBackupAndReap is provided.

.EXAMPLE
.\supervise_workers.ps1 `
  -KeyPath "C:\Users\usr\.ssh\id_key" `
  -SshPort 1234 `
  -StartDownloader `
  -RunDir "D:\yt_download_worker_a" `
  -DownloadArgs @("--workers", "8")

.EXAMPLE
.\supervise_workers.ps1 `
  -KeyPath "C:\Users\usr\.ssh\id_key" `
  -SshPort 1234 `
  -StartDownloader `
  -RunDir "D:\yt_download_worker_a" `
  -DownloadArgs @("--workers", "8") `
  -StartBackupAndReap `
  -BackupDir "O:\ARTS_SoMe-Influence\YT_Download_all_videos\DB_backup" `
  -BackupArgs @("--interval-minutes", "60", "--reap")
#>
[CmdletBinding()]
param(
    [string]$RepoRoot = "",
    [string]$PythonExe = "python",
    [string]$SshExe = "ssh",
    [switch]$SkipTunnel,
    [string]$SshUser = "ucloud",
    [string]$SshHost = "ssh.cloud.sdu.dk",
    [int]$SshPort = 1234,
    [string]$KeyPath = "",
    [int]$LocalDbPort = 15432,
    [string]$RemoteDbHost = "localhost",
    [int]$RemoteDbPort = 5432,
    [string[]]$ExtraSshArgs = @(),
    [string]$DatabaseUrl = "",
    [int]$RestartDelaySeconds = 10,
    [int]$PollIntervalSeconds = 2,
    [string]$SupervisorLogDir = "",
    [switch]$StartDownloader,
    [string]$RunDir = "",
    [string[]]$DownloadArgs = @(),
    [switch]$StartBackupAndReap,
    [string]$BackupDir = "",
    [string[]]$BackupArgs = @(),
    [switch]$StartArchiver,
    [string]$ArchiveDir = "",
    [string[]]$ArchiverArgs = @(),
    [switch]$ValidateOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Log {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[{0}] [{1}] {2}" -f $timestamp, $Level.ToUpperInvariant(), $Message
    Write-Host $line
    Add-Content -LiteralPath $script:SupervisorLogPath -Value $line
}

function Resolve-Executable {
    param([string]$Name)

    $command = Get-Command -Name $Name -ErrorAction Stop | Select-Object -First 1
    return $command.Source
}

function Get-DotEnvValue {
    param(
        [string]$Path,
        [string]$Key
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if (-not $trimmed) {
            continue
        }
        if ($trimmed.StartsWith("#")) {
            continue
        }
        if ($trimmed -match ("^(?:export\s+)?{0}\s*=\s*(.*)$" -f [regex]::Escape($Key))) {
            $value = $Matches[1].Trim()
            if (
                ($value.StartsWith('"') -and $value.EndsWith('"')) -or
                ($value.StartsWith("'") -and $value.EndsWith("'"))
            ) {
                $value = $value.Substring(1, $value.Length - 2)
            }
            return $value
        }
    }

    return $null
}

function Update-DatabaseUrlPort {
    param(
        [string]$Url,
        [string]$Host,
        [int]$Port
    )

    $uri = [Uri]$Url
    $userInfo = if ([string]::IsNullOrWhiteSpace($uri.UserInfo)) { "" } else { "{0}@" -f $uri.UserInfo }
    $authority = "{0}:{1}" -f $Host, $Port
    return "{0}://{1}{2}{3}{4}" -f $uri.Scheme, $userInfo, $authority, $uri.PathAndQuery, $uri.Fragment
}

function Get-DatabaseUrlSummary {
    param([string]$Url)

    try {
        $uri = [Uri]$Url
        return "{0}://{1}:{2}{3}" -f $uri.Scheme, $uri.Host, $uri.Port, $uri.AbsolutePath
    }
    catch {
        return "<unparsed>"
    }
}

function New-ServiceDefinition {
    param(
        [string]$Name,
        [string]$FilePath,
        [string[]]$Arguments,
        [string]$WorkingDirectory,
        [string]$Description
    )

    return [pscustomobject]@{
        Name             = $Name
        FilePath         = $FilePath
        Arguments        = $Arguments
        WorkingDirectory = $WorkingDirectory
        Description      = $Description
        Process          = $null
        NextStartAt      = Get-Date
        StdOutPath       = $null
        StdErrPath       = $null
        LastStartAt      = $null
    }
}

function Start-ServiceInstance {
    param($Service)

    $timestamp = Get-Date -Format "yyyyMMddTHHmmss"
    $stdoutPath = Join-Path $SupervisorLogDir ("{0}_{1}_stdout.log" -f $Service.Name, $timestamp)
    $stderrPath = Join-Path $SupervisorLogDir ("{0}_{1}_stderr.log" -f $Service.Name, $timestamp)

    $process = Start-Process `
        -FilePath $Service.FilePath `
        -ArgumentList $Service.Arguments `
        -WorkingDirectory $Service.WorkingDirectory `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru

    $Service.Process = $process
    $Service.StdOutPath = $stdoutPath
    $Service.StdErrPath = $stderrPath
    $Service.LastStartAt = Get-Date
    $Service.NextStartAt = $null

    Write-Log (
        "{0} started with pid={1}. stdout={2} stderr={3}" -f
        $Service.Name, $process.Id, $stdoutPath, $stderrPath
    )
}

function Stop-ServiceInstance {
    param($Service)

    if ($null -eq $Service.Process) {
        return
    }

    try {
        if (-not $Service.Process.HasExited) {
            Write-Log ("Stopping {0} pid={1}" -f $Service.Name, $Service.Process.Id)
            & taskkill.exe /PID $Service.Process.Id /T /F | Out-Null
        }
    }
    catch {
        Write-Log ("Failed to stop {0}: {1}" -f $Service.Name, $_.Exception.Message) "WARN"
    }
    finally {
        try {
            $Service.Process.Dispose()
        }
        catch {
        }
        $Service.Process = $null
    }
}

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    if (-not [string]::IsNullOrWhiteSpace($PSScriptRoot)) {
        $RepoRoot = $PSScriptRoot
    }
    elseif (-not [string]::IsNullOrWhiteSpace($MyInvocation.MyCommand.Path)) {
        $RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
    }
    else {
        $RepoRoot = (Get-Location).Path
    }
}

$RepoRoot = [System.IO.Path]::GetFullPath($RepoRoot)
if (-not (Test-Path -LiteralPath $RepoRoot)) {
    throw "RepoRoot does not exist: $RepoRoot"
}

if ([string]::IsNullOrWhiteSpace($SupervisorLogDir)) {
    $SupervisorLogDir = Join-Path $RepoRoot "logs\supervisor"
}
$SupervisorLogDir = [System.IO.Path]::GetFullPath($SupervisorLogDir)
New-Item -ItemType Directory -Path $SupervisorLogDir -Force | Out-Null

$script:SupervisorLogPath = Join-Path $SupervisorLogDir ("supervisor_{0}.log" -f (Get-Date -Format "yyyyMMddTHHmmss"))

Write-Log ("Repo root: {0}" -f $RepoRoot)
Write-Log ("Supervisor log: {0}" -f $script:SupervisorLogPath)

if ($RestartDelaySeconds -lt 1) {
    throw "RestartDelaySeconds must be >= 1."
}
if ($PollIntervalSeconds -lt 1) {
    throw "PollIntervalSeconds must be >= 1."
}

if ($StartDownloader -or $StartArchiver) {
    if ([string]::IsNullOrWhiteSpace($RunDir)) {
        throw "RunDir is required when StartDownloader or StartArchiver is used."
    }
}
if ($StartBackupAndReap -and [string]::IsNullOrWhiteSpace($BackupDir)) {
    throw "BackupDir is required when StartBackupAndReap is used."
}
if ($StartArchiver -and [string]::IsNullOrWhiteSpace($ArchiveDir)) {
    throw "ArchiveDir is required when StartArchiver is used."
}

$resolvedPython = $null
if ($StartDownloader -or $StartBackupAndReap -or $StartArchiver) {
    $resolvedPython = Resolve-Executable $PythonExe
    Write-Log ("Python executable: {0}" -f $resolvedPython)
}

$resolvedSsh = $null
if (-not $SkipTunnel) {
    if ([string]::IsNullOrWhiteSpace($KeyPath)) {
        throw "KeyPath is required unless SkipTunnel is set."
    }
    $KeyPath = [System.IO.Path]::GetFullPath($KeyPath)
    if (-not (Test-Path -LiteralPath $KeyPath)) {
        throw "KeyPath does not exist: $KeyPath"
    }
    $resolvedSsh = Resolve-Executable $SshExe
    Write-Log ("SSH executable: {0}" -f $resolvedSsh)
}

$databaseUrlOverride = $null
if (-not [string]::IsNullOrWhiteSpace($DatabaseUrl)) {
    $databaseUrlOverride = $DatabaseUrl
}
elseif (-not $SkipTunnel) {
    $envPath = Join-Path $RepoRoot ".env"
    $dotenvDatabaseUrl = Get-DotEnvValue -Path $envPath -Key "DATABASE_URL"
    if (-not [string]::IsNullOrWhiteSpace($dotenvDatabaseUrl)) {
        try {
            $databaseUrlOverride = Update-DatabaseUrlPort -Url $dotenvDatabaseUrl -Host "localhost" -Port $LocalDbPort
        }
        catch {
            Write-Log "Could not rewrite DATABASE_URL from .env; child processes will use the existing environment or .env as-is." "WARN"
        }
    }
}

if (-not [string]::IsNullOrWhiteSpace($databaseUrlOverride)) {
    $env:DATABASE_URL = $databaseUrlOverride
    Write-Log ("DATABASE_URL override active: {0}" -f (Get-DatabaseUrlSummary $databaseUrlOverride))
}

$services = @()

if (-not $SkipTunnel) {
    $sshArgs = @(
        "-N",
        "-o", "BatchMode=yes",
        "-o", "ServerAliveInterval=60",
        "-o", "ServerAliveCountMax=3",
        "-o", "TCPKeepAlive=yes",
        "-o", "ExitOnForwardFailure=yes",
        "-i", $KeyPath,
        "-L", ("{0}:{1}:{2}" -f $LocalDbPort, $RemoteDbHost, $RemoteDbPort)
    ) + $ExtraSshArgs + @(
        ("{0}@{1}" -f $SshUser, $SshHost),
        "-p", "$SshPort"
    )

    $services += New-ServiceDefinition `
        -Name "ssh_tunnel" `
        -FilePath $resolvedSsh `
        -Arguments $sshArgs `
        -WorkingDirectory $RepoRoot `
        -Description ("SSH tunnel {0}@{1}:{2} -> localhost:{3}" -f $SshUser, $SshHost, $SshPort, $LocalDbPort)
}

if ($StartDownloader) {
    $services += New-ServiceDefinition `
        -Name "start_download" `
        -FilePath $resolvedPython `
        -Arguments @((Join-Path $RepoRoot "start_download.py"), "--run-dir", $RunDir) + $DownloadArgs `
        -WorkingDirectory $RepoRoot `
        -Description ("Downloader worker using run dir {0}" -f $RunDir)
}

if ($StartBackupAndReap) {
    $services += New-ServiceDefinition `
        -Name "backup_and_reap" `
        -FilePath $resolvedPython `
        -Arguments @((Join-Path $RepoRoot "backup_and_reap.py"), "--backup-dir", $BackupDir) + $BackupArgs `
        -WorkingDirectory $RepoRoot `
        -Description ("Backup/reap loop using backup dir {0}" -f $BackupDir)
}

if ($StartArchiver) {
    $services += New-ServiceDefinition `
        -Name "start_archiver" `
        -FilePath $resolvedPython `
        -Arguments @((Join-Path $RepoRoot "start_archiver.py"), "--run-dir", $RunDir, "--archive-dir", $ArchiveDir) + $ArchiverArgs `
        -WorkingDirectory $RepoRoot `
        -Description ("Archiver using run dir {0} and archive dir {1}" -f $RunDir, $ArchiveDir)
}

if ($services.Count -eq 0) {
    throw "Nothing to supervise. Enable at least one service or remove SkipTunnel."
}

Write-Log "Service plan:"
foreach ($service in $services) {
    Write-Log ("  {0}: {1}" -f $service.Name, $service.Description)
}

if ($ValidateOnly) {
    Write-Log "Validation completed. No processes were started."
    return
}

Write-Log "Supervisor started. Press Ctrl+C to stop."

try {
    while ($true) {
        $now = Get-Date
        foreach ($service in $services) {
            if ($null -ne $service.Process) {
                if ($service.Process.HasExited) {
                    $exitCode = $service.Process.ExitCode
                    Write-Log (
                        "{0} exited with code {1}. Restarting in {2}s." -f
                        $service.Name, $exitCode, $RestartDelaySeconds
                    ) "WARN"
                    Write-Log (
                        "{0} last stdout={1} stderr={2}" -f
                        $service.Name, $service.StdOutPath, $service.StdErrPath
                    ) "WARN"
                    try {
                        $service.Process.Dispose()
                    }
                    catch {
                    }
                    $service.Process = $null
                    $service.NextStartAt = (Get-Date).AddSeconds($RestartDelaySeconds)
                }
                continue
            }

            if ($now -ge $service.NextStartAt) {
                try {
                    Start-ServiceInstance -Service $service
                }
                catch {
                    Write-Log (
                        "Failed to start {0}: {1}. Retrying in {2}s." -f
                        $service.Name, $_.Exception.Message, $RestartDelaySeconds
                    ) "ERROR"
                    $service.Process = $null
                    $service.NextStartAt = (Get-Date).AddSeconds($RestartDelaySeconds)
                }
            }
        }

        Start-Sleep -Seconds $PollIntervalSeconds
    }
}
finally {
    Write-Log "Supervisor stopping."
    foreach ($service in $services) {
        Stop-ServiceInstance -Service $service
    }
}
