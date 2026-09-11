#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
python3 - <<'PY'
import json
from pathlib import Path
import subprocess

metadata = json.loads(subprocess.check_output([
    "cargo", "metadata", "--locked", "--offline", "--format-version", "1", "--no-deps",
], text=True))
root = Path(metadata["workspace_root"]).resolve()
core = root / "core"
errors = []
for package in metadata["packages"]:
    if core not in Path(package["manifest_path"]).resolve().parents:
        continue
    for dependency in package["dependencies"]:
        if dependency["kind"] == "dev" or not dependency.get("path"):
            continue
        path = Path(dependency["path"]).resolve()
        if path != core and core not in path.parents:
            errors.append(
                f'{package["name"]}: {dependency["kind"] or "normal"} dependency '
                f'{dependency["name"]} points outside core/: {path}'
            )
if errors:
    raise SystemExit("Core dependency boundary violated:\n" + "\n".join(errors))
print("Core production and build dependencies stay within core/ or external crates")
PY
