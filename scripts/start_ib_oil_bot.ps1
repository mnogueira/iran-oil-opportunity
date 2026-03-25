$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
python scripts/ib_paper_trade_oil.py --allow-live --symbol CL --secondary-symbol BRN --timeframe M15 --bars-count 1500 --poll-seconds 30
