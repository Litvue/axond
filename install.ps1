param(
    [string]$Version = $env:AXOND_VERSION,
    [string]$InstallDir = $env:AXOND_INSTALL_DIR,
    [string]$Target = "x86_64-pc-windows-msvc",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$Repository = if ($env:AXOND_REPOSITORY) { $env:AXOND_REPOSITORY } else { "Litvue/axond" }

if (-not $Version) {
    $headers = @{ "User-Agent" = "axond-installer" }
    $release = Invoke-RestMethod -Headers $headers `
        -Uri "https://api.github.com/repos/$Repository/releases/latest"
    $Version = $release.tag_name -replace '^v', ''
}

if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$') {
    throw "Invalid Axond version: $Version"
}
if ($Target -ne "x86_64-pc-windows-msvc") {
    throw "No prebuilt Windows binary for target $Target"
}
if (-not $InstallDir) {
    $InstallDir = Join-Path ([Environment]::GetFolderPath("LocalApplicationData")) "Programs\axond"
}

$Asset = "axond-$Version-$Target.zip"
$BaseUrl = "https://github.com/$Repository/releases/download/v$Version"

if ($DryRun) {
    "version=$Version"
    "target=$Target"
    "asset=$Asset"
    "install_dir=$InstallDir"
    "url=$BaseUrl/$Asset"
    exit 0
}

$TempDir = Join-Path ([IO.Path]::GetTempPath()) "axond-install-$([guid]::NewGuid())"
New-Item -ItemType Directory -Path $TempDir | Out-Null

try {
    $Archive = Join-Path $TempDir $Asset
    $ChecksumFile = "$Archive.sha256"
    Invoke-WebRequest -Uri "$BaseUrl/$Asset" -OutFile $Archive
    Invoke-WebRequest -Uri "$BaseUrl/$Asset.sha256" -OutFile $ChecksumFile

    $Expected = ((Get-Content -Raw $ChecksumFile).Trim() -split '\s+')[0].ToLowerInvariant()
    $Actual = (Get-FileHash -Path $Archive -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($Actual -ne $Expected) {
        throw "SHA-256 mismatch for $Asset"
    }

    Expand-Archive -Path $Archive -DestinationPath $TempDir
    $Binary = Join-Path $TempDir "axond.exe"
    if (-not (Test-Path -Path $Binary -PathType Leaf)) {
        throw "Release archive did not contain axond.exe"
    }

    New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
    Copy-Item -Force $Binary (Join-Path $InstallDir "axond.exe")
    "Installed axond $Version to $(Join-Path $InstallDir 'axond.exe')"
    "Add $InstallDir to PATH to run axond directly."
}
finally {
    if (Test-Path $TempDir) {
        Remove-Item -Recurse -Force $TempDir
    }
}
