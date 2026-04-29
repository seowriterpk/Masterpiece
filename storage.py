from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_SETTINGS = {
    "max_pages": 150,
    "max_depth": 3,
    "http_concurrency": 12,
    "browser_concurrency": 1,
    "http_timeout": 12,
    "browser_timeout_ms": 15000,
    "max_pages_per_domain": 80,
    "max_candidates_per_page": 18,
    "enable_browser_fallback": True,
    "same_domain_only": True,
    "respect_robots_hint": True,
    "live_refresh_delay": 0.4,
}

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_app_dirs(*dirs: Path) -> None:
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

class LocalStore:
    def __init__(self, data_dir: Path | str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.results_file = self.data_dir / "results.json"
        self.settings_file = self.data_dir / "settings.json"
        self.logs_file = self.data_dir / "logs.jsonl"

    def _read_json(self, path: Path, default: Any) -> Any:
        if not path.exists(): return default
        try: return json.loads(path.read_text(encoding="utf-8"))
        except Exception: return default

    def _write_json(self, path: Path, data: Any) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    def load_settings(self) -> dict:
        existing = self._read_json(self.settings_file, {})
        merged = {**DEFAULT_SETTINGS, **existing}
        self.save_settings(merged)
        return merged

    def save_settings(self, settings: dict) -> None:
        self._write_json(self.settings_file, settings)

    def load_results(self) -> list[dict]:
        rows = self._read_json(self.results_file, [])
        return rows if isinstance(rows, list) else []

    def save_results(self, rows: list[dict]) -> None:
        deduped = {}
        for row in rows:
            key = row.get("normalized_url") or row.get("invite_url")
            if not key: continue
            if key not in deduped:
                deduped[key] = row
            else:
                deduped[key].update({k: v for k, v in row.items() if v not in [None, ""]})
        self._write_json(self.results_file, list(deduped.values()))

    def merge_result_rows(self, rows: list[dict]) -> None:
        current = self.load_results()
        by_key = {r.get("normalized_url") or r.get("invite_url"): r for r in current}
        for row in rows:
            key = row.get("normalized_url") or row.get("invite_url")
            if not key: continue
            if key in by_key: by_key[key].update(row)
            else: by_key[key] = row
        self.save_results(list(by_key.values()))

    def bulk_update_results(self, normalized_urls: list[str], updates: dict) -> None:
        target = set(normalized_urls)
        rows = self.load_results()
        for row in rows:
            if row.get("normalized_url") in target:
                row.update(updates)
                row["updated_at"] = now_iso()
        self.save_results(rows)

    def append_log(self, message: str, data: dict | None = None) -> None:
        item = {"time": now_iso(), "message": message, "data": data or {}}
        with self.logs_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    def load_logs(self, limit: int = 500) -> list[str]:
        if not self.logs_file.exists(): return []
        return self.logs_file.read_text(encoding="utf-8", errors="ignore").splitlines()[-limit:]

    def clear_logs(self) -> None:
        self.logs_file.write_text("", encoding="utf-8")

    def clear_dir(self, path: Path | str) -> None:
        p = Path(path)
        if p.exists(): shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)

    def export_backup_bytes(self) -> bytes:
        backup = {
            "created_at": now_iso(),
            "settings": self.load_settings(),
            "results": self.load_results(),
            "logs": self.load_logs(limit=1000),
        }
        return json.dumps(backup, ensure_ascii=False, indent=2).encode("utf-8")

    def import_backup_bytes(self, raw: bytes) -> None:
        data = json.loads(raw.decode("utf-8", errors="ignore"))
        if "settings" in data and isinstance(data["settings"], dict):
            self.save_settings({**DEFAULT_SETTINGS, **data["settings"]})
        if "results" in data and isinstance(data["results"], list):
            self.save_results(data["results"])
        if "logs" in data and isinstance(data["logs"], list):
            self.logs_file.write_text("\n".join(str(x) for x in data["logs"][-1000:]), encoding="utf-8")

    def reset_all(self) -> None:
        self.save_results([])
        self.save_settings(DEFAULT_SETTINGS)
        self.clear_logs()
