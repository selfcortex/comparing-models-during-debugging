#!/usr/bin/env bash
# Usage: ./new_run.sh base_1      -> creates ../runs/base_1 with a fresh copy of the workspace
set -euo pipefail
name="${1:?run name, e.g. base_1 or lora_1}"
here="$(cd "$(dirname "$0")" && pwd)"
dest="$here/../runs/$name"
[ -e "$dest" ] && { echo "$dest already exists"; exit 1; }
mkdir -p "$dest"
cp "$here/../workspace/taskflow.py" "$here/../workspace/TASK.md" "$dest/"
echo "workspace ready: $dest"
echo "after the agent is done:"
echo "  python3 $here/evaluate.py $dest/taskflow.py --json $here/../results/$name.json"
