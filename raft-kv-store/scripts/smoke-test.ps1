param([string]$Key='',[string]$RequestId='')
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'common.ps1')
try {
    if ($Key -eq '') {$Key='phase12-smoke-'+[guid]::NewGuid().ToString('N')}
    if ($RequestId -eq '') {$RequestId=[guid]::NewGuid().ToString()}
    $bytes=[System.Text.Encoding]::UTF8.GetBytes('phase12-verified-value')
    $base64=[Convert]::ToBase64String($bytes)
    $first=Invoke-NodeRpc -Operation retry-put -Key $Key -RequestId $RequestId -ValueBase64 $base64 -WithValue -ExpectedCode OK
    if ($first.commit_index -le 0) {throw 'First PUT has no positive commit index'}
    $before=Invoke-NodeRpc -Operation status
    $read=Invoke-NodeRpc -Operation get -Key $Key -ExpectedCode RESULT_CODE_OK -ExpectedValueBase64 $base64 -WithExpectedValue
    Assert-Equal True $read.has_value 'GET value presence'
    $retry=Invoke-NodeRpc -Operation put -Key $Key -RequestId $RequestId -ValueBase64 $base64 -WithValue -ExpectedCode RESULT_CODE_OK -ExpectedIndex "$($first.commit_index)"
    $conflict=Invoke-NodeRpc -Operation put -Key $Key -RequestId $RequestId -ValueBase64 ([Convert]::ToBase64String([byte[]](1,2,3))) -WithValue -ExpectedCode RESULT_CODE_REQUEST_ID_CONFLICT
    $after=Invoke-NodeRpc -Operation status
    Assert-Equal $before.last_log_index $after.last_log_index 'No duplicate/conflict WAL entry'
    $emptyKey="$Key-empty";$emptyId=[guid]::NewGuid().ToString()
    $empty=Invoke-NodeRpc -Operation put -Key $emptyKey -RequestId $emptyId -ValueBase64 '' -WithValue -ExpectedCode RESULT_CODE_OK
    $emptyRead=Invoke-NodeRpc -Operation get -Key $emptyKey -ExpectedCode RESULT_CODE_OK -ExpectedValueBase64 '' -WithExpectedValue
    Assert-Equal True $emptyRead.has_value 'Empty value is present'
    Assert-Equal '' $emptyRead.value_base64 'Empty value is empty'
    $deleteId=[guid]::NewGuid().ToString()
    $deleted=Invoke-NodeRpc -Operation delete -Key $Key -RequestId $deleteId -ExpectedCode RESULT_CODE_OK
    $missing=Invoke-NodeRpc -Operation get -Key $Key -ExpectedCode RESULT_CODE_NOT_FOUND
    $sameDelete=Invoke-NodeRpc -Operation delete -Key $Key -RequestId $deleteId -ExpectedCode RESULT_CODE_OK -ExpectedIndex "$($deleted.commit_index)"
    $follower=Invoke-NodeRpc -Operation get -Key $Key -TargetHost node-2 -ExpectedCode RESULT_CODE_NOT_LEADER
    Write-Host "SMOKE PASS key=$Key putIndex=$($first.commit_index) emptyIndex=$($empty.commit_index) deleteIndex=$($deleted.commit_index)"
} catch {Write-Error $_;exit 1}
