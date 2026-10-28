param([Parameter(Mandatory=$true)][ValidateSet('node-1','node-2','node-3')][string]$Node)
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'common.ps1')
try {Invoke-Compose kill --signal SIGKILL $Node;Write-Host "$Node SIGKILL sent; manual restart required"}
catch {Write-Error $_;exit 1}
