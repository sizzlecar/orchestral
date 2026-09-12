[CmdletBinding()]
param([string]$Target = 'x86_64-pc-windows-msvc')
$ErrorActionPreference = 'Stop'
if ($Target -ne 'x86_64-pc-windows-msvc') { throw "Unsupported release target: $Target" }
Set-Location (Join-Path $PSScriptRoot '..')
$metadata = (& cargo metadata --locked --offline --format-version 1 --no-deps | ConvertFrom-Json)
if ($LASTEXITCODE -ne 0) { throw 'Could not read Cargo metadata.' }
$version = ($metadata.packages | Where-Object name -eq 'orchestral-cli').version
& cargo build --locked --release -p orchestral-cli --target $Target
if ($LASTEXITCODE -ne 0) { throw 'Release build failed.' }
$binary = Join-Path $metadata.target_directory "$Target\release\orchestral.exe"
if ((& $binary --version) -ne "orchestral $version") { throw 'Release version mismatch.' }
& $binary --help | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'CLI help smoke failed.' }
& $binary serve --help | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Gateway help smoke failed.' }
$name = "orchestral-v$version-$Target"
$output = Join-Path $metadata.target_directory 'release-artifacts'
$staging = Join-Path ([IO.Path]::GetTempPath()) ('orchestral-package-' + [Guid]::NewGuid().ToString('N'))
try {
    $package = Join-Path $staging $name
    New-Item -ItemType Directory -Force (Join-Path $package 'configs'), $output | Out-Null
    Copy-Item $binary (Join-Path $package 'orchestral.exe')
    Copy-Item LICENSE, README.md, README.zh-CN.md, CHANGELOG.md, RELEASING.md $package
    Copy-Item configs/orchestral.cli.yaml (Join-Path $package 'configs')
    $archive = Join-Path $output "$name.zip"
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    if (Test-Path $archive) { Remove-Item $archive }
    [IO.Compression.ZipFile]::CreateFromDirectory($staging, $archive)
    $hash = (Get-FileHash $archive -Algorithm SHA256).Hash.ToLowerInvariant()
    [IO.File]::WriteAllText("$archive.sha256", "$hash  $name.zip`n", [Text.UTF8Encoding]::new($false))
    Write-Host "Packaged $archive"
} finally {
    Remove-Item $staging -Recurse -Force -ErrorAction SilentlyContinue
}
