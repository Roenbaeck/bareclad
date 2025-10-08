<#!
.SYNOPSIS
  Convenience functions to start/stop the Bareclad server with logging configuration.
.DESCRIPTION
  Provides Start-Bareclad / Stop-Bareclad and a lightweight restart helper.
  Adds an alias 'bareclad-run' for quick invocation in the current terminal.
  You can dot-source this file ( . .\scripts\bareclad.ps1 ) to load the functions
  into your session, or just execute the script to start immediately.
.PARAMETER Log
  Comma separated list of tracing directives (passed to RUST_LOG / bareclad
  tracing subscriber via EnvFilter). Examples:
    'info'               -> info and above for all modules
    'warn,bareclad=info' -> global warn, crate-specific info
    'trace,axum=info'    -> very verbose except axum trimmed
.PARAMETER Profile
  Shortcut presets for common log configurations: quiet | normal | verbose | trace
.EXAMPLE
  # Start with default (normal) logging
  Start-Bareclad
.EXAMPLE
  # Start with custom tracing directives
  Start-Bareclad -Log 'warn,bareclad=info'
.EXAMPLE
  # Restart quickly with verbose logging
  Restart-Bareclad -Profile verbose
#>

[CmdletBinding()] param(
    [string] $AutoStart = 'normal'
)

$script:BARECLAD_PROCESS = $null
$script:BARECLAD_REPO_ROOT = try { Split-Path $PSScriptRoot -Parent } catch { (Resolve-Path '..').Path }

function Set-BarecladLogEnv {
    param(
        [Parameter(Mandatory=$false)][string] $Log,
        [ValidateSet('quiet','normal','verbose','trace')][string] $LogProfile = 'normal'
    )
    if (-not $Log) {
        switch ($Profile) {
            'quiet'   { $Log = 'error' }
            'normal'  { $Log = 'info' }
            'verbose' { $Log = 'debug,bareclad=info' }
            'trace'   { $Log = 'trace' }
        }
    }
    $env:RUST_LOG = $Log
    Write-Host "[bareclad] RUST_LOG set to '$Log'" -ForegroundColor Cyan
}

function Start-Bareclad {
    [CmdletBinding()] param(
        [string] $Log,
        [ValidateSet('quiet','normal','verbose','trace')][string] $LogProfile = 'normal',
        [switch] $ForceRebuild,
        [switch] $Release,
        [switch] $Tail
    )
    if ($script:BARECLAD_PROCESS -and -not $script:BARECLAD_PROCESS.HasExited) {
        Write-Warning 'Bareclad already running. Use Restart-Bareclad or Stop-Bareclad first.'
        return
    }
    Set-BarecladLogEnv -Log $Log -Profile $LogProfile
    $cargoArgs = @('run','--quiet')
    if ($Release) { $cargoArgs = @('run','--release','--quiet') }
    if ($ForceRebuild) { Write-Host '[bareclad] Forcing clean build...' -ForegroundColor Yellow; cargo clean | Out-Null }
    Write-Host "[bareclad] Starting (args: $($cargoArgs -join ' '))..." -ForegroundColor Green
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = 'cargo'
    # Ensure we always point cargo at the repository root (parent of the scripts folder)
    $psi.WorkingDirectory = $script:BARECLAD_REPO_ROOT
    $psi.UseShellExecute = $false
    if ($Tail) {
        # Stream logs directly to this console
        $psi.RedirectStandardOutput = $false
        $psi.RedirectStandardError = $false
    } else {
        # Capture (suppresses live log display)
        $psi.RedirectStandardOutput = $true
        $psi.RedirectStandardError = $true
    }
    # Build a single argument string instead of using ArgumentList (read-only / not settable on some Windows PowerShell versions)
    $psi.Arguments = ($cargoArgs -join ' ')
    $proc = New-Object System.Diagnostics.Process
    $proc.StartInfo = $psi
    $null = $proc.Start()
    $script:BARECLAD_PROCESS = $proc
    if (-not $Tail) {
        # NOTE: Output captured silently. Use -Tail to see logs live. Potential enhancement: background reader.
    }
    Start-Sleep -Milliseconds 600
    if ($proc.HasExited) {
        Write-Warning "[bareclad] Process exited early with code $($proc.ExitCode)"
        Write-Host ($proc.StandardError.ReadToEnd())
        return
    }
    Write-Host "[bareclad] Running (PID: $($proc.Id))" -ForegroundColor Green
}

function Stop-Bareclad {
    [CmdletBinding()] param()
    if (-not $script:BARECLAD_PROCESS) { Write-Host '[bareclad] Not started in this session.'; return }
    if ($script:BARECLAD_PROCESS.HasExited) { Write-Host '[bareclad] Already exited.'; return }
    Write-Host "[bareclad] Stopping PID $($script:BARECLAD_PROCESS.Id)..." -ForegroundColor Yellow
    try {
        $script:BARECLAD_PROCESS.Kill()
        $script:BARECLAD_PROCESS.WaitForExit(3000) | Out-Null
    } catch {
        Write-Warning "[bareclad] Failed to kill process: $_"
    }
}

function Restart-Bareclad {
    [CmdletBinding()] param(
        [string] $Log,
        [ValidateSet('quiet','normal','verbose','trace')][string] $LogProfile = 'normal',
        [switch] $ForceRebuild,
        [switch] $Release
    )
    Stop-Bareclad
    Start-Bareclad -Log $Log -Profile $LogProfile -ForceRebuild:$ForceRebuild -Release:$Release
}

Set-Alias bareclad-run Start-Bareclad

if ($MyInvocation.InvocationName -ne '.') {
    # If script is executed directly, start with provided AutoStart profile
    Start-Bareclad -Profile $AutoStart
}
