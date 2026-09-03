# Windows port of run_setup.sh, using pixi instead of poetry.
#
# Assumes MONICA has been built at $PathToMonicaBinDir (monica-zmq-proxy.exe /
# monica-zmq-server.exe) and that this script's directory is a direct subfolder of the
# amei_exercises repo root, whose pyproject.toml defines the pixi environment
# run-producer.py / run-consumer.py depend on (zmq, zalfmas_common, ...).

$PathToMonicaBinDir = "C:\Users\berg\monica\build"
$MonicaParameters = "C:\Users\berg\monica-parameters"  # adjust if this lives elsewhere
$env:MONICA_PARAMETERS = $MonicaParameters
Write-Output $env:MONICA_PARAMETERS

$RepoRoot = Split-Path -Parent $PSScriptRoot
$PixiManifest = Join-Path $RepoRoot "pyproject.toml"

# run-producer.py / run-consumer.py use paths relative to this folder (./data/, sim.json, ...)
Set-Location $PSScriptRoot

function Start-Detached
{
    param([string]$FilePath, [string[]]$ArgumentList)
    return Start-Process -FilePath $FilePath -ArgumentList $ArgumentList -WindowStyle Minimized -PassThru
}

$inProxy = Start-Detached "$PathToMonicaBinDir\monica-zmq-proxy.exe" @("-pps", "-f", "6666", "-b", "6677")
Write-Output "in_proxy_pid -> $($inProxy.Id)"
$outProxy = Start-Detached "$PathToMonicaBinDir\monica-zmq-proxy.exe" @("-pps", "-f", "7788", "-b", "7777")
Write-Output "out_proxy_pid -> $($outProxy.Id)"

$monicaProcs = @()
for ($i = 1; $i -le 5; $i++)
{
    $monicaProcs += Start-Detached "$PathToMonicaBinDir\monica-zmq-server.exe" @("-ci", "-i", "tcp://localhost:6677", "-co", "-o", "tcp://localhost:7788")
}
Write-Output "monica_pids -> $($monicaProcs.Id -join ' ')"

Write-Output "run producer"
$producer = Start-Process -FilePath "pixi" -ArgumentList @("run", "--manifest-path", $PixiManifest, "python", "run-producer.py") -WindowStyle Minimized -PassThru

Write-Output "run consumer"
$consumers = @()
for ($i = 1; $i -le 1; $i++)
{
    $consumers += Start-Process -FilePath "pixi" -ArgumentList @("run", "--manifest-path", $PixiManifest, "python", "run-consumer.py") -WindowStyle Minimized -PassThru
}
# last consumer runs attached/in the foreground, like the original script, so this blocks here
& pixi run --manifest-path $PixiManifest python run-consumer.py

Write-Output "consumer finished -> kill all servers and proxies"
Start-Sleep -Seconds 120

Stop-Process -Id $inProxy.Id -ErrorAction SilentlyContinue
Write-Output "killed in_proxy_pid -> $($inProxy.Id)"
Stop-Process -Id $outProxy.Id -ErrorAction SilentlyContinue
Write-Output "killed out_proxy_pid -> $($outProxy.Id)"
foreach ($p in $monicaProcs)
{
    Stop-Process -Id $p.Id -ErrorAction SilentlyContinue
    Write-Output "killed monica_pid -> $($p.Id)"
}
