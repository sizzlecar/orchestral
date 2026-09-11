# Install an official Orchestral release without administrator privileges.
[CmdletBinding()]
param(
    [string]$Version,
    [string]$InstallDir = (Join-Path $env:LOCALAPPDATA 'Orchestral\bin'),
    [switch]$NoModifyPath,
    [string]$ReleaseBaseUrl = $env:ORCHESTRAL_RELEASE_BASE_URL
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
$scratch = $null
$staged = $null
$installerFile = $MyInvocation.MyCommand.Path
try {
    $architecture = $env:PROCESSOR_ARCHITEW6432
    if (-not $architecture) { $architecture = $env:PROCESSOR_ARCHITECTURE }
    if ($architecture -ne 'AMD64') {
        throw 'This installer supports Windows x64. See https://orch.pandaailabs.com for supported platforms.'
    }
    $releaseOrigin = 'https://orch.pandaailabs.com'
    if (-not $Version) {
        try {
            $Version = (Invoke-RestMethod "$releaseOrigin/releases/latest/x86_64-pc-windows-msvc.txt" -TimeoutSec 30).Trim()
        } catch {
            throw 'Could not find the Windows release. Check your connection and retry, or download the Windows package at https://orch.pandaailabs.com/#install.'
        }
    }
    $Version = $Version -replace '^v', ''
    if ($Version -notmatch '^\d+\.\d+\.\d+$') { throw 'Version must be MAJOR.MINOR.PATCH, for example 0.3.0.' }
    $archive = "orchestral-v$Version-x86_64-pc-windows-msvc"
    $asset = "$archive.zip"
    if (-not $ReleaseBaseUrl) { $ReleaseBaseUrl = "$releaseOrigin/releases/v$Version" }
    $uri = [Uri]$ReleaseBaseUrl
    if ($uri.Scheme -ne 'https' -and -not ($uri.Scheme -eq 'http' -and $uri.IsLoopback)) {
        throw 'Release mirror must use HTTPS (loopback HTTP is allowed for local testing).'
    }
    if ($uri.UserInfo -or $uri.Query -or $uri.Fragment) { throw 'Release mirror URL must not contain credentials, a query, or a fragment.' }
    $ReleaseBaseUrl = $ReleaseBaseUrl.TrimEnd('/')
    $scratch = Join-Path ([IO.Path]::GetTempPath()) ('orchestral-install-' + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory $scratch | Out-Null
    Write-Host "Downloading Orchestral $Version for Windows x64..."
    foreach ($filename in @($asset, "$asset.sha256")) {
        try {
            Invoke-WebRequest "$ReleaseBaseUrl/$filename" -UseBasicParsing -OutFile (Join-Path $scratch $filename) -TimeoutSec 300
        } catch {
            throw "Could not download $filename. Check the version, connection and release assets. Your current installation is unchanged."
        }
    }
    $expected = ((Get-Content (Join-Path $scratch "$asset.sha256") -First 1) -split '\s+')[0]
    if ($expected -notmatch '^[a-fA-F0-9]{64}$') { throw 'The checksum file is invalid. Your current installation is unchanged.' }
    $actual = (Get-FileHash (Join-Path $scratch $asset) -Algorithm SHA256).Hash
    if ($actual -ne $expected) { throw 'Checksum mismatch. Nothing was installed; download again from the official release.' }
    Write-Host 'Checksum verified.'
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [IO.Compression.ZipFile]::OpenRead((Join-Path $scratch $asset))
    try {
        # Windows PowerShell 5.1's ZIP writer uses backslashes in entry names.
        $entries = @($zip.Entries | Where-Object { $_.FullName.Replace('\', '/') -eq "$archive/orchestral.exe" })
        if ($entries.Count -ne 1) { throw 'Release archive must contain exactly one expected executable.' }
        $entry = $entries[0]
        $binary = Join-Path $scratch 'orchestral.exe'
        [IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $binary)
    } finally { $zip.Dispose() }
    $reportedVersion = & $binary --version
    if ($LASTEXITCODE -ne 0 -or $reportedVersion -ne "orchestral $Version") { throw 'Downloaded executable did not report the expected version.' }
    if (-not $InstallDir.Trim()) { throw 'Installation directory must not be empty.' }
    $InstallDir = [IO.Path]::GetFullPath($InstallDir)
    New-Item -ItemType Directory -Force $InstallDir | Out-Null
    $staged = Join-Path $InstallDir ('.orchestral-install-' + [Guid]::NewGuid().ToString('N') + '.exe')
    Copy-Item $binary $staged
    $destination = Join-Path $InstallDir 'orchestral.exe'
    try {
        if (Test-Path -LiteralPath $destination) {
            [IO.File]::Replace($staged, $destination, [System.Management.Automation.Language.NullString]::Value)
        } else { [IO.File]::Move($staged, $destination) }
    } catch {
        throw 'Could not replace orchestral.exe. Close any running Orchestral processes and retry. Your previous installation is preserved.'
    }
    $staged = $null
    Write-Host "Installed Orchestral $Version at $destination"
    if (-not $NoModifyPath) {
        $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
        if (($userPath -split ';') -notcontains $InstallDir) {
            [Environment]::SetEnvironmentVariable('Path', ($InstallDir + ';' + $userPath).TrimEnd(';'), 'User')
            Write-Host 'Added the installation directory to your user PATH.'
        }
        if (($env:Path -split ';') -notcontains $InstallDir) { $env:Path = "$InstallDir;$env:Path" }
        Write-Host "`nStart in your project directory:`n  orchestral"
        Write-Host 'Other open terminals may need to be reopened to refresh PATH.'
    } else {
        Write-Host "`nAdd $InstallDir to PATH, or run:"
        Write-Host ("  & '" + $destination.Replace("'", "''") + "'")
    }
    Write-Host "`nSetup and local models: https://orch.pandaailabs.com/#quickstart"
    $global:LASTEXITCODE = 0
} catch {
    [Console]::Error.WriteLine("Orchestral install: " + $_.Exception.Message)
    $global:LASTEXITCODE = 1
    if ($installerFile) { exit 1 }
    return
} finally {
    if ($staged -and (Test-Path -LiteralPath $staged)) { Remove-Item -LiteralPath $staged -Force }
    if ($scratch -and (Test-Path -LiteralPath $scratch)) { Remove-Item -LiteralPath $scratch -Recurse -Force }
}
