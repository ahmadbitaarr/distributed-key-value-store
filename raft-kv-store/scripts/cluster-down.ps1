$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'common.ps1')
try {
    Invoke-Compose down
    Write-Host 'Containers stopped and removed; named WAL volumes preserved (no -v).'
} catch {Write-Error $_;exit 1}
