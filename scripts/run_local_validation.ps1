$ErrorActionPreference = "Stop"

python generators/generate_batch.py --customers 100 --products 30 --orders 300 --seed 42
python generators/send_events.py --dry-run --events 100 --seed 42
python -m compileall src generators tools notebooks
python -m pytest

