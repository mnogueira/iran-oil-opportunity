$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
python scripts/ib_paper_trade_oil.py --symbol CL --secondary-symbol BRN --timeframe M15 --bars-count 1500 --poll-seconds 30
