"""Runs the shared acceptance harness (rask-sdk-acceptance) and talks to its control API."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
LANG = "python"


def find_harness() -> Path | None:
    configured = os.environ.get("RASK_SDK_ACCEPTANCE_DIR")
    if configured:
        return Path(configured)
    # A checkout next to this one, or next to the repository this worktree belongs to.
    for base in (REPO.parent, REPO.parent.parent):
        candidate = base / "rask-sdk-acceptance"
        if (candidate / "bin" / "rask-sdk-acceptance.mjs").exists():
            return candidate
    return None


class Harness:
    """One harness process on free ports. It runs one scenario at a time."""

    def __init__(self, directory: Path, prefix: str | None = None) -> None:
        extra = ["--prefix", prefix] if prefix else []
        self.process = subprocess.Popen(
            ["node", str(directory / "bin" / "rask-sdk-acceptance.mjs"), "--port", "0", *extra],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        assert self.process.stdout is not None
        banner = ""
        for _ in range(10):
            banner += self.process.stdout.readline()
            if "scenarios" in banner:
                break
        found = re.search(r"control\s+(http://\S+)", banner)
        if not found:
            self.stop()
            raise RuntimeError(f"The harness did not start: {banner}")
        self.control_url = found.group(1).rstrip("/")
        health = self.get("/health")
        self.health = health
        self.base_url: str = health.get("base_url") or re.search(r"base_url\s+(\S+)", banner).group(1)  # type: ignore[union-attr]
        self.api_key: str = re.search(r"test API key\s+(\S+)", banner).group(1)  # type: ignore[union-attr]
        self.other_url: str = re.search(r"other host\s+(\S+)", banner).group(1)  # type: ignore[union-attr]

    def stop(self) -> None:
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()

    def _send(self, method: str, path: str, body: Any = None) -> Any:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(  # noqa: S310
            f"{self.control_url}{path}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"} if data is not None else {},
        )
        last: Exception | None = None
        for _ in range(20):
            try:
                with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
                    return json.loads(response.read() or b"null")
            except urllib.error.HTTPError as exc:
                raise RuntimeError(f"{method} {path}: {exc.code} {exc.read().decode()[:1000]}") from exc
            except urllib.error.URLError as exc:
                last = exc
                time.sleep(0.1)
        raise RuntimeError(f"{method} {path}: {last}")

    def get(self, path: str) -> Any:
        return self._send("GET", path)

    def post(self, path: str, body: Any = None) -> Any:
        return self._send("POST", path, {} if body is None else body)

    def scenarios(self) -> list[dict[str, Any]]:
        listing = self.get(f"/scenarios?lang={LANG}")
        return listing["scenarios"] if isinstance(listing, dict) else listing
