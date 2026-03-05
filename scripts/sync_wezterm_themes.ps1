param(
    [ValidateSet("Auto", "x64", "x86", "both")]
    [string]$Architecture = "Auto",
    [switch]$Clean,
    [string]$RepoOwner = "mbadolato",
    [string]$RepoName = "iTerm2-Color-Schemes",
    [string]$Branch = "master"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-WithRetry {
    param(
        [scriptblock]$Action,
        [int]$Attempts = 3
    )

    for ($i = 1; $i -le $Attempts; $i++) {
        try {
            return & $Action
        } catch {
            if ($i -ge $Attempts) {
                throw
            }
            Start-Sleep -Seconds $i
        }
    }
}

function Get-InstallCandidates {
    $candidates = @()

    if ($env:ProgramFiles) {
        $installDir = Join-Path $env:ProgramFiles "Rusty"
        $exePath = Join-Path $installDir "rusty.exe"
        $termPath = Join-Path $installDir "term"
        $candidates += [pscustomobject]@{
            Arch       = "x64"
            InstallDir = $installDir
            ExePath    = $exePath
            TermPath   = $termPath
            Exists     = Test-Path $installDir
            ExeExists  = Test-Path $exePath
            ExeMtime   = if (Test-Path $exePath) { (Get-Item $exePath).LastWriteTimeUtc } else { [datetime]::MinValue }
        }
    }

    if (${env:ProgramFiles(x86)}) {
        $installDir = Join-Path ${env:ProgramFiles(x86)} "Rusty"
        $exePath = Join-Path $installDir "rusty.exe"
        $termPath = Join-Path $installDir "term"
        $candidates += [pscustomobject]@{
            Arch       = "x86"
            InstallDir = $installDir
            ExePath    = $exePath
            TermPath   = $termPath
            Exists     = Test-Path $installDir
            ExeExists  = Test-Path $exePath
            ExeMtime   = if (Test-Path $exePath) { (Get-Item $exePath).LastWriteTimeUtc } else { [datetime]::MinValue }
        }
    }

    return @($candidates | Where-Object { $_.Exists })
}

function Resolve-Targets {
    param(
        [string]$ArchSelection
    )

    $installed = @(Get-InstallCandidates)
    if ($installed.Count -eq 0) {
        throw "Rusty was not found in Program Files or Program Files (x86)."
    }

    if ($ArchSelection -eq "both") {
        return $installed
    }

    if ($ArchSelection -eq "x64" -or $ArchSelection -eq "x86") {
        $target = $installed | Where-Object { $_.Arch -eq $ArchSelection } | Select-Object -First 1
        if (-not $target) {
            throw "Rusty $ArchSelection install was not found."
        }
        return @($target)
    }

    if ($installed.Count -eq 1) {
        return @($installed[0])
    }

    $x64 = $installed | Where-Object { $_.Arch -eq "x64" } | Select-Object -First 1
    $x86 = $installed | Where-Object { $_.Arch -eq "x86" } | Select-Object -First 1

    if ($x64 -and $x86) {
        if ($x64.ExeExists -and $x86.ExeExists -and $x64.ExeMtime -ne $x86.ExeMtime) {
            $picked = if ($x64.ExeMtime -gt $x86.ExeMtime) { $x64 } else { $x86 }
            Write-Warning "Both x64 and x86 installs were found. Auto mode picked the most recently updated install: $($picked.Arch)."
            return @($picked)
        }
        Write-Warning "Both x64 and x86 installs were found. Auto mode defaulted to x64."
        return @($x64)
    }

    return @($installed[0])
}

function Assert-WritableDirectory {
    param(
        [string]$Path
    )

    try {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
        $probe = Join-Path $Path ".rusty-write-probe"
        Set-Content -Path $probe -Value "ok" -Encoding ASCII
        Remove-Item -Path $probe -Force
    } catch {
        throw "Cannot write to '$Path'. Run this script as Administrator. $($_.Exception.Message)"
    }
}

function Get-WeztermThemeFiles {
    param(
        [string]$Owner,
        [string]$Repository,
        [string]$Ref
    )

    $headers = @{
        "Accept"     = "application/vnd.github+json"
        "User-Agent" = "Rusty-Wezterm-Theme-Sync"
    }

    $url = "https://api.github.com/repos/$Owner/$Repository/contents/wezterm?ref=$Ref"
    $response = Invoke-WithRetry { Invoke-RestMethod -Uri $url -Headers $headers -Method Get }
    $items = @($response)
    $files = @($items | Where-Object { $_.type -eq "file" -and $_.name -like "*.toml" -and $_.download_url })

    if ($files.Count -eq 0) {
        throw "No .toml files were returned from $url"
    }

    return $files
}

function Sync-ThemesToPath {
    param(
        [string]$TargetPath,
        [object[]]$ThemeFiles,
        [switch]$RemoveExtra
    )

    New-Item -ItemType Directory -Path $TargetPath -Force | Out-Null

    $headers = @{
        "User-Agent" = "Rusty-Wezterm-Theme-Sync"
    }

    $downloaded = 0
    $totalToDownload = @($ThemeFiles).Count
    $upstreamNames = New-Object System.Collections.Generic.HashSet[string]([System.StringComparer]::OrdinalIgnoreCase)

    foreach ($file in $ThemeFiles) {
        $null = $upstreamNames.Add($file.name)
        $outFile = Join-Path $TargetPath $file.name
        Invoke-WithRetry {
            Invoke-WebRequest -Uri $file.download_url -Headers $headers -OutFile $outFile
        }
        $downloaded++
        Write-Host ("    [{0}/{1}] downloaded {2}" -f $downloaded, $totalToDownload, $file.name)
    }

    $removed = 0
    if ($RemoveExtra) {
        $existing = Get-ChildItem -Path $TargetPath -File -Filter *.toml -ErrorAction SilentlyContinue
        foreach ($entry in $existing) {
            if (-not $upstreamNames.Contains($entry.Name)) {
                Remove-Item -Path $entry.FullName -Force
                $removed++
            }
        }
    }

    return [pscustomobject]@{
        Downloaded = $downloaded
        Removed    = $removed
        TotalNow   = (Get-ChildItem -Path $TargetPath -File -Filter *.toml -ErrorAction SilentlyContinue).Count
    }
}

$targets = Resolve-Targets -ArchSelection $Architecture
$themes = Get-WeztermThemeFiles -Owner $RepoOwner -Repository $RepoName -Ref $Branch

foreach ($target in $targets) {
    Assert-WritableDirectory -Path $target.TermPath
    $result = Sync-ThemesToPath -TargetPath $target.TermPath -ThemeFiles $themes -RemoveExtra:$Clean
    Write-Host ("[{0}] Synced {1} file(s) to {2}. total={3}{4}" -f
        $target.Arch,
        $result.Downloaded,
        $target.TermPath,
        $result.TotalNow,
        $(if ($Clean) { ", removed=$($result.Removed)" } else { "" }))
}
