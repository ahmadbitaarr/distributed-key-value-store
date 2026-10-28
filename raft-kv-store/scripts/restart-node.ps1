param([Parameter(Mandatory=$true)][ValidateSet('node-1','node-2','node-3')][string]$Node)
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'common.ps1')
try {Invoke-Compose restart $Node;Wait-NodeHealth $Node;Write-Host "$Node healthy after restart"}
catch {Write-Error $_;exit 1}
