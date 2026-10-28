$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'common.ps1')
try {
    Invoke-Compose ps
    foreach ($n in @('node-1','node-2','node-3')) {
        Write-Host "$n health=$(Get-NodeHealth $n)"
        if ($n -eq 'node-1') { Write-Host "Leader read state:"; Invoke-NodeRpc -Operation get -Key '__raftkv_deployment_health_probe__' | ConvertTo-Json -Compress }
        else {Invoke-NodeRpc -Operation status -TargetHost $n | ConvertTo-Json -Compress}
    }
} catch {Write-Error $_;exit 1}
