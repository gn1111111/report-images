# ---------- EDIT THESE ----------
$server   = "WF-RDS-RSH-21\BIDEV"       # e.g. "SQL01" or "SQL01\INST1"
$database = "Test"
$folder   = "C:\Users\GNyamundanda\London Borough of Waltham Forest\Test - Test - Current"  # folder containing .png files
$mimeType = "image/png"

# Windows auth (common)
$connectionString = "Server=$server;Database=$database;Integrated Security=True;TrustServerCertificate=True;"
# If SQL auth instead, use:
# $connectionString = "Server=$server;Database=$database;User ID=YOURUSER;Password=YOURPASS;TrustServerCertificate=True;"

# ---------- SQL (parameterized UPSERT) ----------
$mergeSql = @"
MERGE dbo.ReportImages AS t
USING (SELECT @ImageKey AS ImageKey) AS s
ON t.ImageKey = s.ImageKey
WHEN MATCHED THEN
  UPDATE SET FileName = @FileName,
             MimeType = @MimeType,
             ImageBytes = @ImageBytes,
             LastUpdatedUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT (ImageKey, FileName, MimeType, ImageBytes)
  VALUES (@ImageKey, @FileName, @MimeType, @ImageBytes);
"@

$conn = New-Object System.Data.SqlClient.SqlConnection($connectionString)
$conn.Open()

# Prepare command once (faster)
$cmd  = New-Object System.Data.SqlClient.SqlCommand($mergeSql, $conn)
$null = $cmd.Parameters.Add("@ImageKey",   [System.Data.SqlDbType]::NVarChar, 200)
$null = $cmd.Parameters.Add("@FileName",   [System.Data.SqlDbType]::NVarChar, 260)
$null = $cmd.Parameters.Add("@MimeType",   [System.Data.SqlDbType]::NVarChar, 50)
$null = $cmd.Parameters.Add("@ImageBytes", [System.Data.SqlDbType]::VarBinary, -1)

# Optional: wrap in a transaction for all-or-nothing
$tx = $conn.BeginTransaction()
$cmd.Transaction = $tx

$files = Get-ChildItem -Path $folder -Filter *.png -File
if ($files.Count -eq 0) { throw "No PNG files found in $folder" }

$success = 0
$failed  = 0

foreach ($f in $files) {
    try {
        $bytes = [System.IO.File]::ReadAllBytes($f.FullName)
        if ($bytes.Length -eq 0) { throw "File is empty." }

        # ImageKey = filename without extension
        $imageKey = $f.BaseName

        $cmd.Parameters["@ImageKey"].Value   = $imageKey
        $cmd.Parameters["@FileName"].Value   = $f.Name
        $cmd.Parameters["@MimeType"].Value   = $mimeType
        $cmd.Parameters["@ImageBytes"].Value = $bytes

        $cmd.ExecuteNonQuery() | Out-Null
        $success++
        Write-Host "OK  $imageKey  ($($bytes.Length) bytes)"
    }
    catch {
        $failed++
        Write-Host "ERR $($f.Name): $($_.Exception.Message)"
    }
}

if ($failed -gt 0) {
    $tx.Rollback()
    $conn.Close()
    throw "Upload failed for $failed file(s). Transaction rolled back. Fix errors and re-run."
} else {
    $tx.Commit()
    $conn.Close()
    Write-Host "Done. Uploaded $success PNG(s)."
}