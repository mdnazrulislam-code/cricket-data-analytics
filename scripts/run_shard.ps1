param(
    [Parameter(Mandatory = $true)][int]$Worker,
    [int]$Workers = 5
)

Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle = "Archive $Worker of $Workers  1971-today"

while ($true) {
    Write-Host "---- shard $Worker/$Workers starting $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ----"
    & python -u .\scripts\collect_archive.py --from-year 1971 --to-year 2026 --shard "$Worker/$Workers"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "---- shard $Worker/$Workers finished ----"
        break
    }
    Write-Host "---- shard $Worker/$Workers stopped (exit $LASTEXITCODE). Resuming from the last saved match in 15 seconds ----"
    Start-Sleep -Seconds 15
}
