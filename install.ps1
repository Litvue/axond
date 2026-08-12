param(
    [string]$Version = $env:AXOND_VERSION,
    [string]$InstallDir = $env:AXOND_INSTALL_DIR,
    [string]$Target = "x86_64-pc-windows-msvc",
    [switch]$RequireAttestation,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$Repository = if ($env:AXOND_REPOSITORY) { $env:AXOND_REPOSITORY } else { "Litvue/axond" }
$AttestationSetting = if ($env:AXOND_REQUIRE_ATTESTATION) {
    $env:AXOND_REQUIRE_ATTESTATION.ToLowerInvariant()
}
else {
    "0"
}
switch ($AttestationSetting) {
    { $_ -in "1", "true", "yes", "on" } { $AttestationFromEnvironment = $true; break }
    { $_ -in "0", "false", "no", "off" } { $AttestationFromEnvironment = $false; break }
    default {
        throw "AXOND_REQUIRE_ATTESTATION must be 1/0, true/false, yes/no, or on/off"
    }
}
$MustVerifyAttestation = $RequireAttestation -or $AttestationFromEnvironment

if ($PSVersionTable.PSEdition -eq "Desktop") {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    $WebRequestParameters = @{ UseBasicParsing = $true }
}
else {
    $WebRequestParameters = @{}
}

if (-not $Version) {
    Add-Type -AssemblyName System.Net.Http
    $LatestHandler = [Net.Http.HttpClientHandler]::new()
    $LatestHandler.AllowAutoRedirect = $true
    $LatestClient = [Net.Http.HttpClient]::new($LatestHandler)
    $LatestRequest = [Net.Http.HttpRequestMessage]::new(
        [Net.Http.HttpMethod]::Head,
        "https://github.com/$Repository/releases/latest"
    )
    $LatestRequest.Headers.UserAgent.ParseAdd("axond-installer")
    try {
        $LatestResponse = $LatestClient.SendAsync($LatestRequest).GetAwaiter().GetResult()
        $LatestResponse.EnsureSuccessStatusCode() | Out-Null
        $LatestUrl = $LatestResponse.RequestMessage.RequestUri.AbsoluteUri
    }
    finally {
        if ($LatestResponse) { $LatestResponse.Dispose() }
        $LatestRequest.Dispose()
        $LatestClient.Dispose()
        $LatestHandler.Dispose()
    }
    $Version = ($LatestUrl.TrimEnd('/') -split '/')[-1] -replace '^v', ''
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
    "require_attestation=$MustVerifyAttestation"
    "url=$BaseUrl/$Asset"
    exit 0
}

$TempDir = Join-Path ([IO.Path]::GetTempPath()) "axond-install-$([guid]::NewGuid())"
New-Item -ItemType Directory -Path $TempDir | Out-Null

try {
    $Archive = Join-Path $TempDir $Asset
    $ChecksumFile = "$Archive.sha256"
    Invoke-WebRequest @WebRequestParameters -Uri "$BaseUrl/$Asset" -OutFile $Archive
    Invoke-WebRequest @WebRequestParameters -Uri "$BaseUrl/$Asset.sha256" -OutFile $ChecksumFile

    $Expected = ((Get-Content -Raw $ChecksumFile).Trim() -split '\s+')[0].ToLowerInvariant()
    $Actual = (Get-FileHash -Path $Archive -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($Actual -ne $Expected) {
        throw "SHA-256 mismatch for $Asset"
    }

    if ($MustVerifyAttestation) {
        if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
            throw "GitHub CLI is required by -RequireAttestation or AXOND_REQUIRE_ATTESTATION=1"
        }
        & gh auth status *> $null
        if ($LASTEXITCODE -ne 0) {
            throw "Authenticated GitHub CLI is required by -RequireAttestation or AXOND_REQUIRE_ATTESTATION=1"
        }
        & gh attestation --help *> $null
        if ($LASTEXITCODE -ne 0) {
            throw "GitHub CLI with attestation support is required by -RequireAttestation or AXOND_REQUIRE_ATTESTATION=1"
        }
        "Verifying GitHub build provenance for $Asset"
        & gh attestation verify $Archive --repo $Repository
        if ($LASTEXITCODE -ne 0) {
            throw "GitHub attestation verification failed for $Asset"
        }
    }
    else {
        Write-Warning "Checksum verified; use -RequireAttestation to verify GitHub build provenance."
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
