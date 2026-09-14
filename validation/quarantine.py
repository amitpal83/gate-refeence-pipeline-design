"""Dead-letter handling for batches rejected by hard ingestion checks."""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def quarantine_batch(paths: Iterable[str | Path], quarantine_root: str | Path, reason: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    destination = Path(quarantine_root) / stamp
    destination.mkdir(parents=True, exist_ok=True)
    copied = []
    for path_value in paths:
        source = Path(path_value)
        if source.exists():
            target = destination / source.name
            shutil.copy2(source, target)
            copied.append(str(target))
    (destination / "failure.json").write_text(
        json.dumps({"reason": reason, "files": copied}, indent=2),
        encoding="utf-8",
    )
    return str(destination)