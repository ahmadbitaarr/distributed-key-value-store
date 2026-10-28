$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'common.ps1')
try {
    Invoke-Compose up '--detach' '--build'
    Wait-NodeHealth node-2
    Wait-NodeHealth node-3
    Wait-NodeHealth node-1
    Invoke-Compose ps
    Write-Host 'Three-node cluster healthy (leader read-ready; quorum checked by smoke test).'
} catch {Write-Error $_;exit 1}
