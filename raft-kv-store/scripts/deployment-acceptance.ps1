# Live acceptance: modifies the local Compose test cluster, exercises crashes and restarts.
# Does not delete volumes. Run only with development/test WALs.
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'common.ps1')
try {
    Invoke-Compose up '--detach' '--build'
    foreach($n in @('node-2','node-3','node-1')) {Wait-NodeHealth $n}

    # Real Linux runtime is non-root, and mounted named WAL is writable.
    $uid=@(Invoke-Compose exec '-T' node-1 id -u)[-1]
    Assert-Equal '10001' $uid 'Non-root UID'
    Invoke-Compose exec '-T' node-1 sh '-c' 'test -w /var/lib/raftkv && test ! -e /opt/raftkv/src && ! command -v mvn >/dev/null'

    # Real WAL lease cannot be acquired by a second node process using the same volume.
    # Launch with a different port is not permitted by ClusterConfig; the failure must be lease-related.
    $oldErrorPreference=$ErrorActionPreference
    $ErrorActionPreference='Continue' # A lease rejection is expected from the child JVM.
    try { $leaseOutput=@(& docker compose --project-directory $script:ModuleRoot -f (Join-Path $script:ModuleRoot 'compose.yaml') exec -T node-1 java -jar /opt/raftkv/app.jar 2>&1); $leaseExit=$LASTEXITCODE }
    finally {$ErrorActionPreference=$oldErrorPreference}
    if ($leaseExit -eq 0) {throw 'Second NodeMain unexpectedly acquired active WAL directory'}
    if (($leaseOutput -join ' ') -notmatch '(?i)lease|locked|lock|WAL directory already owned') {throw "Second NodeMain failed for another reason: $leaseOutput"}

    & (Join-Path $PSScriptRoot 'smoke-test.ps1')
    if ($LASTEXITCODE -ne 0) {throw 'Base smoke assertions failed'}

    $key='phase12-accept-'+[guid]::NewGuid().ToString('N')
    $id=[guid]::NewGuid().ToString()
    $payload=[Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes('persistent-p12'))
    $put=Invoke-NodeRpc -Operation retry-put -Key $key -RequestId $id -ValueBase64 $payload -WithValue -ExpectedCode RESULT_CODE_OK
    $originalIndex=[long]$put.commit_index
    if($originalIndex -le 0){throw 'No first commit index'}
    $priorLog=Invoke-NodeRpc -Operation status

    # One follower lost: write majority n1+n2 remains; n3 catches up without another client mutation.
    Invoke-Compose stop node-3
    $majorityKey="$key-majority"
    $majorityPut=Invoke-NodeRpc -Operation put -Key $majorityKey -RequestId ([guid]::NewGuid().ToString()) -ValueBase64 $payload -WithValue -ExpectedCode RESULT_CODE_OK
    Invoke-Compose start node-3
    Wait-NodeHealth node-3
    Wait-FollowerCatchUp -Follower node-3 -MinimumIndex ([long]$majorityPut.commit_index)

    # Leader graceful restart: witness is required, committed content and dedup index unchanged.
    Invoke-Compose restart node-1
    Wait-NodeHealth node-1
    Invoke-NodeRpc -Operation get -Key $key -ExpectedCode RESULT_CODE_OK -ExpectedValueBase64 $payload -WithExpectedValue | Out-Null
    Invoke-NodeRpc -Operation put -Key $key -RequestId $id -ValueBase64 $payload -WithValue -ExpectedCode RESULT_CODE_OK -ExpectedIndex "$originalIndex" | Out-Null

    # Hard leader SIGKILL then replay/reconciliation; exact retry cannot append.
    Invoke-Compose kill --signal SIGKILL node-1
    Invoke-Compose start node-1
    Wait-NodeHealth node-1
    $beforeRetry=Invoke-NodeRpc -Operation status
    Invoke-NodeRpc -Operation put -Key $key -RequestId $id -ValueBase64 $payload -WithValue -ExpectedCode RESULT_CODE_OK -ExpectedIndex "$originalIndex" | Out-Null
    $afterRetry=Invoke-NodeRpc -Operation status
    Assert-Equal $beforeRetry.last_log_index $afterRetry.last_log_index 'Hard-crash duplicate must not append'
    Invoke-NodeRpc -Operation get -Key $key -ExpectedCode RESULT_CODE_OK -ExpectedValueBase64 $payload -WithExpectedValue | Out-Null

    # The leader cannot become READY based only on local durable WAL, when both followers are absent.
    Invoke-Compose stop node-2 node-3
    $noQuorumKey="$key-noquorum"
    $q=Invoke-NodeRpc -Operation put -Key $noQuorumKey -RequestId ([guid]::NewGuid().ToString()) -ValueBase64 $payload -WithValue
    if($q.code -notin @('NOT_READY','NO_QUORUM')) {throw "Write without majority incorrectly returned $($q.code)"}
    Invoke-NodeRpc -Operation get -Key $noQuorumKey -ExpectedCode RESULT_CODE_NOT_FOUND | Out-Null
    Invoke-Compose restart node-1
    # Container liveness differs from readiness; check actual RPC admission while peers remain absent.
    $unready=Invoke-NodeRpc -Operation get -Key $key
    Assert-Equal 'NOT_READY' $unready.code 'Recovered leader without witness'
    $health=Get-NodeHealth node-1
    if($health -eq 'healthy') {throw 'Witnessless recovered leader was marked healthy'}
    Invoke-Compose start node-2
    Wait-NodeHealth node-2
    Wait-NodeHealth node-1
    Invoke-NodeRpc -Operation get -Key $key -ExpectedCode RESULT_CODE_OK -ExpectedValueBase64 $payload -WithExpectedValue | Out-Null
    Invoke-Compose start node-3
    Wait-NodeHealth node-3

    # Full recreation with named volumes retained. NEVER down -v.
    Invoke-Compose down
    Invoke-Compose up '--detach'
    foreach($n in @('node-2','node-3','node-1')) {Wait-NodeHealth $n}
    $afterRecreate=Invoke-NodeRpc -Operation status
    Invoke-NodeRpc -Operation get -Key $key -ExpectedCode RESULT_CODE_OK -ExpectedValueBase64 $payload -WithExpectedValue | Out-Null
    Invoke-NodeRpc -Operation put -Key $key -RequestId $id -ValueBase64 $payload -WithValue -ExpectedCode RESULT_CODE_OK -ExpectedIndex "$originalIndex" | Out-Null
    $afterDuplicate=Invoke-NodeRpc -Operation status
    Assert-Equal $afterRecreate.last_log_index $afterDuplicate.last_log_index 'Recreation retry must not append'

    # Rebuild and force-recreate using exactly the same volumes; original logical effect stays singular.
    Invoke-Compose up '--detach' '--build' '--force-recreate'
    foreach($n in @('node-2','node-3','node-1')) {Wait-NodeHealth $n}
    $beforeRebuildRetry=Invoke-NodeRpc -Operation status
    Invoke-NodeRpc -Operation put -Key $key -RequestId $id -ValueBase64 $payload -WithValue -ExpectedCode RESULT_CODE_OK -ExpectedIndex "$originalIndex" | Out-Null
    $afterRebuildRetry=Invoke-NodeRpc -Operation status
    Assert-Equal $beforeRebuildRetry.last_log_index $afterRebuildRetry.last_log_index 'Image rebuild retry must not append'
    Write-Host "DEPLOYMENT ACCEPTANCE PASS initialIndex=$originalIndex majorityIndex=$($majorityPut.commit_index) durableLastIndex=$($afterRebuildRetry.last_log_index)"
} catch {
    Write-Error $_
    Write-Host 'Cluster and named WAL volumes preserved for forensic inspection (no automatic cleanup).'
    exit 1
}
