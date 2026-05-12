<#
.SYNOPSIS
    Install swytch on Windows.

.DESCRIPTION
    iwr -useb https://raw.githubusercontent.com/swytchdb/swytch/main/scripts/install.ps1 | iex

.PARAMETER Version
    Release tag to install (e.g. v0.1.0). Defaults to latest.

.PARAMETER InstallDir
    Install directory. Defaults to $env:LOCALAPPDATA\Programs\swytch.

.PARAMETER SkipVerify
    Skip checksum verification (not recommended).

.PARAMETER AddToPath
    Add InstallDir to the user PATH if not already present.
#>
[CmdletBinding()]
param(
    [string]$Version = $env:SWYTCH_VERSION,
    [string]$InstallDir = $env:SWYTCH_INSTALL,
    [switch]$SkipVerify,
    [switch]$AddToPath
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$Repo = 'swytchdb/swytch'
$Bin  = 'swytch'

function Info($msg) { Write-Host "install: $msg" }
function Fail($msg) { Write-Error "install: $msg"; exit 1 }

$arch = switch ($env:PROCESSOR_ARCHITECTURE) {
    'AMD64' { 'x86_64' }
    'ARM64' { 'arm64' }
    default { Fail "unsupported architecture: $env:PROCESSOR_ARCHITECTURE" }
}

if (-not $Version) {
    Info 'resolving latest release'
    $latest = Invoke-RestMethod "https://api.github.com/repos/$Repo/releases/latest"    $Version = $latest.tag_name
    if (-not $Version) { Fail 'could not determine latest version' }
}
$versionNum = $Version.TrimStart('v')
if (-not $Version.StartsWith('v')) { $Version = "v$Version" }

$archive   = "${Bin}_${versionNum}_windows_${arch}.zip"
$base      = "https://github.com/$Repo/releases/download/$Version"
$url       = "$base/$archive"
$sumsUrl   = "$base/checksums.txt"

$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("swytch-" + [Guid]::NewGuid())
New-Item -ItemType Directory -Path $tmp -Force | Out-Null

try {
    Info "downloading $archive"
    $archivePath = Join-Path $tmp $archive
    Invoke-WebRequest -Uri $url -OutFile $archivePath
    if (-not $SkipVerify) {
        Info 'verifying checksum'
        $sumsPath = Join-Path $tmp 'checksums.txt'
        Invoke-WebRequest -Uri $sumsUrl -OutFile $sumsPath
        $pattern = '^([0-9a-fA-F]+)\s+' + [regex]::Escape($archive) + '$'
        $expected = Select-String -Path $sumsPath -Pattern $pattern |
            ForEach-Object { $_.Matches[0].Groups[1].Value } |
            Select-Object -First 1
        if (-not $expected) { Fail "no checksum entry for $archive" }

        $actual = (Get-FileHash -Path $archivePath -Algorithm SHA256).Hash.ToLower()
        if ($actual -ne $expected.ToLower()) {
            Fail "checksum mismatch (expected $expected, got $actual)"
        }
    }

    Info 'extracting'
    Expand-Archive -Path $archivePath -DestinationPath $tmp -Force
    $exe = Join-Path $tmp "$Bin.exe"
    if (-not (Test-Path $exe)) { Fail "expected $Bin.exe not found in archive" }

    if (-not $InstallDir) {
        $InstallDir = Join-Path $env:LOCALAPPDATA "Programs\swytch"
    }
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    Move-Item -Path $exe -Destination (Join-Path $InstallDir "$Bin.exe") -Force

    Info "installed $Bin $Version to $InstallDir\$Bin.exe"

    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    $onPath = ($userPath -split ';') -contains $InstallDir
    if (-not $onPath) {
        if ($AddToPath) {
            [Environment]::SetEnvironmentVariable('Path', "$userPath;$InstallDir", 'User')
            Info "added $InstallDir to user PATH (open a new shell to use it)"
        } else {
            Info "warning: $InstallDir is not on PATH"
            Info "         re-run with -AddToPath to fix, or add it manually"
        }
    }
}
finally {
    Remove-Item -Path $tmp -Recurse -Force -ErrorAction SilentlyContinue
}
