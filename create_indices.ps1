param(
  [Parameter(Mandatory = $false)]
  [string]$Endpoint = "http://localhost:9200",

  [Parameter(Mandatory = $false)]
  [string]$MappingFile = ".\\opensearch_mapping_v1.json",

  [Parameter(Mandatory = $false)]
  [string]$Username = "",

  [Parameter(Mandatory = $false)]
  [string]$Password = "",

  [Parameter(Mandatory = $false)]
  [switch]$DeleteIfExists,

  [Parameter(Mandatory = $false)]
  [switch]$Insecure
)

$ErrorActionPreference = "Stop"

if ($Insecure) {
  [System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
  [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
}

if (!(Test-Path $MappingFile)) {
  throw "Mapping file not found: $MappingFile"
}

$mappingRaw = Get-Content -Raw -Encoding UTF8 $MappingFile
try {
  $mapping = $mappingRaw | ConvertFrom-Json -Depth 100
} catch {
  # Windows PowerShell 5.1 does not support -Depth on ConvertFrom-Json
  $mapping = $mappingRaw | ConvertFrom-Json
}
if (-not $mapping.indices) {
  throw "Invalid mapping file: missing 'indices' root."
}

function Invoke-OsRequest {
  param(
    [string]$Method,
    [string]$Uri,
    [string]$Body = $null
  )

  $headers = @{ "Content-Type" = "application/json" }
  if ($Username -and $Password) {
    $pair = "{0}:{1}" -f $Username, $Password
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($pair)
    $base64 = [System.Convert]::ToBase64String($bytes)
    $headers["Authorization"] = "Basic $base64"
  }

  if ($null -ne $Body) {
    return Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers -Body $Body
  }
  return Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers
}

function Get-ErrorText {
  param($Err)
  $msg = ""
  if ($Err -and $Err.Exception -and $Err.Exception.Message) {
    $msg = $Err.Exception.Message
  }
  if ($Err -and $Err.ErrorDetails -and $Err.ErrorDetails.Message) {
    $msg = "$msg $($Err.ErrorDetails.Message)"
  }
  return $msg
}

foreach ($indexProp in $mapping.indices.PSObject.Properties) {
  $indexName = $indexProp.Name
  $indexDef = $indexProp.Value
  $indexUri = "$Endpoint/$indexName"

  if ($DeleteIfExists) {
    try {
      Write-Host "Deleting existing index if present: $indexName"
      Invoke-OsRequest -Method "DELETE" -Uri $indexUri | Out-Null
    } catch {
      $errText = Get-ErrorText $_
      if ($errText -match "index_not_found_exception") {
        # ignore
      } else {
        throw
      }
    }
  }

  $body = @{
    settings = $indexDef.settings
    mappings = $indexDef.mappings
  } | ConvertTo-Json -Depth 100

  Write-Host "Creating index: $indexName"
  try {
    $resp = Invoke-OsRequest -Method "PUT" -Uri $indexUri -Body $body
    Write-Host ("Created: {0}, acknowledged={1}" -f $indexName, $resp.acknowledged)
  } catch {
    $errText = Get-ErrorText $_
    if ($errText -match "resource_already_exists_exception") {
      Write-Host "Index already exists, skip: $indexName"
      continue
    }
    throw
  }
}

Write-Host "All done."
