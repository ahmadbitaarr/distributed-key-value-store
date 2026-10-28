param([ValidateSet('node-1','node-2','node-3','all')][string]$Node='all',[int]$Tail=100,[switch]$Follow)
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'common.ps1')
try {
    $argsList=@('logs','--tail',"$Tail")
    if ($Follow) {$argsList+='--follow'}
    if ($Node -ne 'all') {$argsList+=$Node}
    Invoke-Compose @argsList
} catch {Write-Error $_;exit 1}
