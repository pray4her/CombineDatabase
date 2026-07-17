param(
  [Parameter(Mandatory = $false)]
  [string]$Endpoint = "http://localhost:9200",

  [Parameter(Mandatory = $false)]
  [string]$BulkDir = ".\\output_schema_v1",

  [Parameter(Mandatory = $false)]
  [string]$Username = "",

  [Parameter(Mandatory = $false)]
  [string]$Password = "",

  [Parameter(Mandatory = $false)]
  [string[]]$IncludeFiles = @(),

  [Parameter(Mandatory = $false)]
  [switch]$Refresh,

  [Parameter(Mandatory = $false)]
  [switch]$Insecure,

  [Parameter(Mandatory = $false)]
  [int]$ChunkLines = 10000,

  [Parameter(Mandatory = $false)]
  [int]$MaxRetries = 5,

  [Parameter(Mandatory = $false)]
  [int]$RetryBackoffSeconds = 2,

  [Parameter(Mandatory = $false)]
  [int]$Concurrency = 2
)

$ErrorActionPreference = "Stop"

if ($ChunkLines -lt 2 -or ($ChunkLines % 2 -ne 0)) {
  throw "ChunkLines must be an even integer >= 2. Current: $ChunkLines"
}
if ($Concurrency -lt 1) {
  throw "Concurrency must be >= 1."
}

if ($Insecure) {
  [System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
  [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
}

if (!(Test-Path $BulkDir)) {
  throw "Bulk directory not found: $BulkDir"
}

$bulkFiles = Get-ChildItem -Path $BulkDir -Filter "*.bulk.ndjson" | Sort-Object Name
if ($IncludeFiles.Count -gt 0) {
  $includeSet = @{}
  foreach ($item in $IncludeFiles) {
    if ($null -eq $item) { continue }
    foreach ($token in ($item -split ",")) {
      $name = $token.Trim()
      if ($name) {
        $includeSet[$name] = $true
      }
    }
  }
  $bulkFiles = $bulkFiles | Where-Object { $includeSet.ContainsKey($_.Name) }
}
if ($bulkFiles.Count -eq 0) {
  throw "No *.bulk.ndjson files found in $BulkDir"
}

$headers = @{
  "Content-Type" = "application/x-ndjson"
}

if ($Username -and $Password) {
  $pair = "{0}:{1}" -f $Username, $Password
  $bytes = [System.Text.Encoding]::UTF8.GetBytes($pair)
  $base64 = [System.Convert]::ToBase64String($bytes)
  $headers["Authorization"] = "Basic $base64"
}

$refreshParam = if ($Refresh) { "true" } else { "false" }
$bulkUri = "$Endpoint/_bulk?refresh=$refreshParam"

Write-Host ("Bulk files: {0}, Concurrency: {1}, ChunkLines: {2}" -f $bulkFiles.Count, $Concurrency, $ChunkLines)

$worker = {
  param(
    [string]$FilePath,
    [string]$BulkUri,
    [string]$Username,
    [string]$Password,
    [bool]$Insecure,
    [int]$ChunkLines,
    [int]$MaxRetries,
    [int]$RetryBackoffSeconds,
    [string]$BulkDir
  )

  $ErrorActionPreference = "Stop"
  if ($Insecure) {
    [System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
  }

  $headers = @{ "Content-Type" = "application/x-ndjson" }
  if ($Username -and $Password) {
    $pair = "{0}:{1}" -f $Username, $Password
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($pair)
    $base64 = [System.Convert]::ToBase64String($bytes)
    $headers["Authorization"] = "Basic $base64"
  }

  function Invoke-BulkWithRetry {
    param(
      [string]$Body,
      [int]$ChunkNo
    )
    $bodyBytes = [System.Text.Encoding]::UTF8.GetBytes($Body)
    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
      try {
        return Invoke-RestMethod -Method "POST" -Uri $BulkUri -Headers $headers -Body $bodyBytes
      } catch {
        $statusCode = $null
        if ($_.Exception -and $_.Exception.Response -and $_.Exception.Response.StatusCode) {
          $statusCode = [int]$_.Exception.Response.StatusCode
        }
        $retryable = ($statusCode -in @(408, 409, 425, 429, 500, 502, 503, 504))
        $msg = $_.Exception.Message
        if (-not $retryable -and $msg) {
          if ($msg -match "timed out|timeout|temporar|connection|reset|refused") {
            $retryable = $true
          }
        }
        if (($attempt -lt $MaxRetries) -and $retryable) {
          $sleepSec = [math]::Min(60, [int]([math]::Pow($RetryBackoffSeconds, $attempt)))
          Start-Sleep -Seconds $sleepSec
          continue
        }
        throw
      }
    }
  }

  $file = Get-Item -LiteralPath $FilePath
  $name = $file.Name
  $totalLines = 0
  $srCount = [System.IO.File]::OpenText($file.FullName)
  try {
    while ($null -ne $srCount.ReadLine()) { $totalLines++ }
  } finally {
    $srCount.Close()
    $srCount.Dispose()
  }
  if ($totalLines -eq 0) {
    return [PSCustomObject]@{
      file = $name
      total_lines = 0
      total_chunks = 0
      imported_docs = 0
      failed_items = 0
      failure_log = $null
      note = "empty file"
    }
  }

  $totalChunks = [math]::Ceiling($totalLines / $ChunkLines)
  $chunkNo = 0
  $importedDocs = 0
  $failures = New-Object System.Collections.Generic.List[object]
  $lineBuffer = New-Object System.Collections.Generic.List[string]

  $sr = [System.IO.File]::OpenText($file.FullName)
  try {
    while ($true) {
      $line = $sr.ReadLine()
      if ($null -eq $line) {
        break
      }
      $lineBuffer.Add($line)
      if ($lineBuffer.Count -ge $ChunkLines) {
        $chunkNo++
        $body = ([string]::Join("`n", $lineBuffer.ToArray())) + "`n"
        $resp = Invoke-BulkWithRetry -Body $body -ChunkNo $chunkNo
        $itemsCount = if ($resp.items) { $resp.items.Count } else { [int]($lineBuffer.Count / 2) }
        $importedDocs += $itemsCount

        if ($resp.errors) {
          $i = 0
          foreach ($item in $resp.items) {
            $i++
            foreach ($op in @("index", "create", "update", "delete")) {
              if ($null -ne $item.$op) {
                $entry = $item.$op
                if ($entry.error) {
                  $failures.Add([PSCustomObject]@{
                    file = $name
                    chunk_no = $chunkNo
                    item_no = $i
                    op = $op
                    index = $entry._index
                    id = $entry._id
                    status = $entry.status
                    error_type = $entry.error.type
                    error_reason = $entry.error.reason
                  }) | Out-Null
                }
              }
            }
          }
        }
        $lineBuffer.Clear()
      }
    }

    if ($lineBuffer.Count -gt 0) {
      $chunkNo++
      $body = ([string]::Join("`n", $lineBuffer.ToArray())) + "`n"
      $resp = Invoke-BulkWithRetry -Body $body -ChunkNo $chunkNo
      $itemsCount = if ($resp.items) { $resp.items.Count } else { [int]($lineBuffer.Count / 2) }
      $importedDocs += $itemsCount

      if ($resp.errors) {
        $i = 0
        foreach ($item in $resp.items) {
          $i++
          foreach ($op in @("index", "create", "update", "delete")) {
            if ($null -ne $item.$op) {
              $entry = $item.$op
              if ($entry.error) {
                $failures.Add([PSCustomObject]@{
                  file = $name
                  chunk_no = $chunkNo
                  item_no = $i
                  op = $op
                  index = $entry._index
                  id = $entry._id
                  status = $entry.status
                  error_type = $entry.error.type
                  error_reason = $entry.error.reason
                }) | Out-Null
              }
            }
          }
        }
      }
      $lineBuffer.Clear()
    }
  } finally {
    $sr.Close()
    $sr.Dispose()
  }

  $failurePath = $null
  if ($failures.Count -gt 0) {
    $ts = Get-Date -Format "yyyyMMdd_HHmmss"
    $failurePath = Join-Path $BulkDir ("bulk_failures_{0}_{1}.json" -f $file.BaseName, $ts)
    $failures | ConvertTo-Json -Depth 10 | Out-File -FilePath $failurePath -Encoding UTF8
  }

  return [PSCustomObject]@{
    file = $name
    total_lines = $totalLines
    total_chunks = $chunkNo
    imported_docs = $importedDocs
    failed_items = $failures.Count
    failure_log = $failurePath
  }
}

$queue = [System.Collections.Queue]::new()
foreach ($file in $bulkFiles) { $queue.Enqueue($file) }
$jobs = New-Object System.Collections.Generic.List[object]
$results = New-Object System.Collections.Generic.List[object]

while (($queue.Count -gt 0) -or ($jobs.Count -gt 0)) {
  while (($queue.Count -gt 0) -and ($jobs.Count -lt $Concurrency)) {
    $file = $queue.Dequeue()
    Write-Host ("Start importing: {0}" -f $file.FullName)
    $j = Start-Job -ScriptBlock $worker -ArgumentList @(
      $file.FullName, $bulkUri, $Username, $Password, [bool]$Insecure, $ChunkLines, $MaxRetries, $RetryBackoffSeconds, $BulkDir
    )
    $jobs.Add($j) | Out-Null
  }

  for ($idx = $jobs.Count - 1; $idx -ge 0; $idx--) {
    $j = $jobs[$idx]
    if ($j.State -in @("Completed", "Failed", "Stopped")) {
      if ($j.State -eq "Completed") {
        $res = Receive-Job -Job $j
        if ($res) {
          foreach ($row in @($res)) {
            $results.Add($row) | Out-Null
            Write-Host ("Done: {0}, docs={1}, chunks={2}, failed_items={3}" -f $row.file, $row.imported_docs, $row.total_chunks, $row.failed_items)
            if ($row.failure_log) {
              Write-Host ("  Failure log: {0}" -f $row.failure_log)
            }
          }
        }
      } else {
        Write-Host ("Job failed: {0}" -f $j.Id)
        Receive-Job -Job $j -ErrorAction SilentlyContinue | Out-Null
      }
      Remove-Job -Job $j -Force
      $jobs.RemoveAt($idx)
    }
  }

  $done = $results.Count
  $total = $bulkFiles.Count
  $percent = if ($total -gt 0) { [int](($done / $total) * 100) } else { 100 }
  Write-Progress -Activity "Bulk import files" -Status ("{0}/{1} files completed" -f $done, $total) -PercentComplete $percent
  Start-Sleep -Milliseconds 500
}

Write-Progress -Activity "Bulk import files" -Completed

$totalDocs = 0
$totalFailed = 0
foreach ($r in $results) {
  $totalDocs += [int]$r.imported_docs
  $totalFailed += [int]$r.failed_items
}
Write-Host ("Bulk import completed. files={0}, imported_docs={1}, failed_items={2}" -f $results.Count, $totalDocs, $totalFailed)
