param(
  [Parameter(Mandatory = $false)]
  [string]$InputPng = (Join-Path $PSScriptRoot "icon.png"),

  [Parameter(Mandatory = $false)]
  [string]$OutputIco = (Join-Path $PSScriptRoot "icon.ico"),

  # Common Windows icon sizes. 256 is stored with width/height byte set to 0 in ICO.
  [Parameter(Mandatory = $false)]
  [int[]]$Sizes = @(16, 32, 48, 256)
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (!(Test-Path $InputPng)) {
  throw "Input PNG not found: $InputPng"
}

Add-Type -AssemblyName System.Drawing

function New-PngBytesFromBitmap([System.Drawing.Bitmap]$bmp) {
  $ms = New-Object System.IO.MemoryStream
  try {
    $bmp.Save($ms, [System.Drawing.Imaging.ImageFormat]::Png)
    return ,$ms.ToArray()
  } finally {
    $ms.Dispose()
  }
}

# Load source image
$srcImg = [System.Drawing.Image]::FromFile($InputPng)
try {
  $entries = @()

  foreach ($s in $Sizes) {
    if ($s -le 0) { continue }

    $bmp = New-Object System.Drawing.Bitmap $s, $s, ([System.Drawing.Imaging.PixelFormat]::Format32bppArgb)
    try {
      $g = [System.Drawing.Graphics]::FromImage($bmp)
      try {
        $g.Clear([System.Drawing.Color]::Transparent)
        $g.CompositingMode = [System.Drawing.Drawing2D.CompositingMode]::SourceOver
        $g.CompositingQuality = [System.Drawing.Drawing2D.CompositingQuality]::HighQuality
        $g.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
        $g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::HighQuality
        $g.PixelOffsetMode = [System.Drawing.Drawing2D.PixelOffsetMode]::HighQuality
        $g.DrawImage($srcImg, 0, 0, $s, $s)
      } finally {
        $g.Dispose()
      }

      $pngBytes = New-PngBytesFromBitmap $bmp
      $entries += [PSCustomObject]@{
        Size = $s
        Png  = $pngBytes
      }
    } finally {
      $bmp.Dispose()
    }
  }

  if ($entries.Count -eq 0) {
    throw "No sizes generated. Check -Sizes."
  }

  # Write ICO (PNG-compressed images inside ICO).
  $fs = New-Object System.IO.FileStream($OutputIco, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
  $bw = New-Object System.IO.BinaryWriter($fs)
  try {
    # ICONDIR
    $bw.Write([UInt16]0)         # reserved
    $bw.Write([UInt16]1)         # type = icon
    $bw.Write([UInt16]$entries.Count)

    $dirSize = 6 + (16 * $entries.Count)
    $offset = $dirSize

    # ICONDIRENTRY table
    foreach ($e in $entries) {
      $wByte = if ($e.Size -ge 256) { 0 } else { [byte]$e.Size }
      $hByte = if ($e.Size -ge 256) { 0 } else { [byte]$e.Size }

      $bw.Write([byte]$wByte)       # width
      $bw.Write([byte]$hByte)       # height
      $bw.Write([byte]0)            # color count
      $bw.Write([byte]0)            # reserved
      $bw.Write([UInt16]1)          # planes
      $bw.Write([UInt16]32)         # bitcount
      $bw.Write([UInt32]$e.Png.Length) # bytes in resource
      $bw.Write([UInt32]$offset)    # image offset

      $offset += $e.Png.Length
    }

    # Image data blobs
    foreach ($e in $entries) {
      $bw.Write($e.Png)
    }
  } finally {
    $bw.Dispose()
    $fs.Dispose()
  }

  Write-Host "Wrote ICO: $OutputIco"
} finally {
  $srcImg.Dispose()
}

