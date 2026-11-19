# Windows port of run_setup.sh, using pixi instead of poetry.
#
# Assumes MONICA has been built at $PathToMonicaBinDir (monica-zmq-proxy.exe /
# monica-zmq-server.exe) and that this script's directory is a direct subfolder of the
# amei_exercises repo root, whose pyproject.toml defines the pixi environment
# run-producer.py / run-consumer.py depend on (zmq, zalfmas_common, ...).
#
# Only a single consumer is started (unlike the original run_setup.sh's 5): the out-proxy
# round-robins messages, including the final "all envs sent" sentinel, across every
# connected consumer, so with multiple consumers only one of them ever sees that sentinel.
# The script blocks on the *last* consumer it starts to know when the run is done - if that
# one isn't the lucky one, it sits on its 10-minute recv timeout even after every other
# consumer (if any) already received all the data. A single consumer can't lose that race.

$PathToMonicaBinDir = "C:\Users\berg\GitHub\monica\build-release"
$MonicaParameters = "C:\Users\berg\GitHub\monica-parameters"  # adjust if this lives elsewhere
$env:MONICA_PARAMETERS = $MonicaParameters
Write-Output $env:MONICA_PARAMETERS

$RepoRoot = Split-Path -Parent $PSScriptRoot
$PixiManifest = Join-Path $RepoRoot "pyproject.toml"

# run-producer.py / run-consumer.py use paths relative to this folder (./data/, sim.json, ...)
Set-Location $PSScriptRoot

# All processes below run with -WindowStyle Minimized, so nothing they print is visible
# anywhere unless captured - redirect stdout/stderr to files here instead, so a failure
# (wrong exe path, MONICA_PARAMETERS not found, connection refused, ...) is actually
# diagnosable instead of silently vanishing in a minimized window nobody's watching.
$LogDir = Join-Path $PSScriptRoot "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Write-Output "logs -> $LogDir"

function Start-Detached
{
    param([string]$Name, [string]$FilePath, [string[]]$ArgumentList)
    return Start-Process -FilePath $FilePath -ArgumentList $ArgumentList -WindowStyle Minimized -PassThru `
        -RedirectStandardOutput (Join-Path $LogDir "$Name.out.log") `
        -RedirectStandardError (Join-Path $LogDir "$Name.err.log")
}

$inProxy = Start-Detached "in_proxy" "$PathToMonicaBinDir\monica-zmq-proxy.exe" @("-pps", "-f", "6666", "-b", "6677")
Write-Output "in_proxy_pid -> $($inProxy.Id)"
$outProxy = Start-Detached "out_proxy" "$PathToMonicaBinDir\monica-zmq-proxy.exe" @("-pps", "-f", "7788", "-b", "7777")
Write-Output "out_proxy_pid -> $($outProxy.Id)"

# give the proxies a moment to actually bind before servers/producer try to reach them
Start-Sleep -Seconds 2

$monicaProcs = @()
for ($i = 1; $i -le 4; $i++)
{
    $monicaProcs += Start-Detached "monica_$i" "$PathToMonicaBinDir\monica-zmq-server.exe" @("-ci", "-i", "tcp://localhost:6677", "-co", "-o", "tcp://localhost:7788")
}
Write-Output "monica_pids -> $($monicaProcs.Id -join ' ')"

# give the servers a moment to connect to the proxy before the producer starts pushing
Start-Sleep -Seconds 3

Write-Output "run producer"
$producer = Start-Detached "producer" "pixi" @("run", "--manifest-path", $PixiManifest, "python", "run-producer.py")

Write-Output "run consumer"
# runs attached/in the foreground so this blocks here until it has received everything
& pixi run --manifest-path $PixiManifest python run-consumer.py

Write-Output "consumer finished -> kill all servers and proxies"

Stop-Process -Id $inProxy.Id -ErrorAction SilentlyContinue
Write-Output "killed in_proxy_pid -> $($inProxy.Id)"
Stop-Process -Id $outProxy.Id -ErrorAction SilentlyContinue
Write-Output "killed out_proxy_pid -> $($outProxy.Id)"
foreach ($p in $monicaProcs)
{
    Stop-Process -Id $p.Id -ErrorAction SilentlyContinue
    Write-Output "killed monica_pid -> $($p.Id)"
}
