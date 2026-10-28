Set-StrictMode -Version Latest
$script:ModuleRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

function Assert-NodeName([string]$Node) {
    if ($Node -notin @('node-1','node-2','node-3')) { throw "Node must be node-1, node-2, or node-3: $Node" }
}
function Invoke-Compose {
    param([Parameter(ValueFromRemainingArguments=$true)][string[]]$ComposeArgs)
    & docker compose --project-directory $script:ModuleRoot -f (Join-Path $script:ModuleRoot 'compose.yaml') @ComposeArgs
    if ($LASTEXITCODE -ne 0) {throw "docker compose $($ComposeArgs -join ' ') failed ($LASTEXITCODE)"}
}
function Get-ContainerId([string]$Node) {
    Assert-NodeName $Node
    $ids = @(Invoke-Compose ps -q --all $Node)
    if ($ids.Count -ne 1 -or [string]::IsNullOrWhiteSpace([string]$ids[0])) {throw "No unique container for $Node"}
    return ([string]$ids[0]).Trim()
}
function Get-NodeHealth([string]$Node) {
    $id=Get-ContainerId $Node
    $status=& docker inspect --format '{{.State.Health.Status}}' $id
    if ($LASTEXITCODE -ne 0) {throw "Cannot inspect $Node health"}
    return ([string]$status).Trim()
}
function Wait-NodeHealth {
    param([string]$Node,[int]$TimeoutSeconds=90)
    $until=(Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $health=Get-NodeHealth $Node
        if ($health -eq 'healthy') {return}
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $until)
    throw "$Node never became healthy: $health"
}
function Invoke-NodeRpc {
    param([Parameter(Mandatory=$true)][string]$Operation,
          [string]$Key='', [string]$RequestId='', [string]$ValueBase64='',
          [switch]$WithValue, [string]$TargetHost='node-1', [string]$Runner='node-1',
          [string]$ExpectedCode='', [string]$ExpectedIndex='', [string]$ExpectedValueBase64='',
          [switch]$WithExpectedValue)
    Assert-NodeName $Runner
    $argsList=@('exec','-T',$Runner,'java','-cp','/opt/raftkv/app.jar',
        'io.github.ahmadbitaarr.raftkv.client.DeploymentSmokeClient',$Operation,
        "--host=$TargetHost",'--port=50051','--leader=n1')
    if ($Key -ne '') {$argsList+= "--key=$Key"}
    if ($RequestId -ne '') {$argsList+= "--request-id=$RequestId"}
    if ($WithValue) {$argsList+= "--value-base64=$ValueBase64"}
    if ($ExpectedCode -ne '') {$argsList+= "--expect=$ExpectedCode"}
    if ($ExpectedIndex -ne '') {$argsList+= "--expect-index=$ExpectedIndex"}
    if ($WithExpectedValue) {$argsList+= "--expect-value-base64=$ExpectedValueBase64"}
    $lines=@(Invoke-Compose @argsList)
    if ($lines.Count -lt 1) {throw "No smoke-client JSON from $Operation"}
    $json=([string]$lines[-1]).Trim()
    try {return ($json | ConvertFrom-Json -ErrorAction Stop)}
    catch {throw "Invalid smoke-client JSON: $json"}
}
function Assert-Equal($Expected,$Actual,[string]$Description) {
    if ([string]$Expected -cne [string]$Actual) {throw "$Description expected '$Expected', got '$Actual'"}
}
function Wait-FollowerCatchUp {
    param([string]$Follower,[long]$MinimumIndex,[int]$TimeoutSeconds=90)
    $until=(Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $s=Invoke-NodeRpc -Operation status -TargetHost $Follower
        if ($s.last_log_index -ge $MinimumIndex -and $s.durable_commit_index -ge $MinimumIndex -and $s.applied_index -ge $MinimumIndex) {return}
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $until)
    throw "$Follower did not durably catch up through $MinimumIndex"
}
