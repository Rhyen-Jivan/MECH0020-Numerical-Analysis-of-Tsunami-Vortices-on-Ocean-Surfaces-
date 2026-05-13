#!/usr/bin/env python3
"""iCUE collector runtime.

Reads iCUE sensor logging CSV output when available and falls back to
config/discovery metadata to expose device and metric coverage.
"""

from __future__ import annotations

import csv
import json
import re
from collections import deque
from pathlib import Path
from typing import Dict, Iterable, List, Optional


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(value: object) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    text = re.sub(r"[^0-9eE+\-\.]", "", text)
    if not text or text in {".", "-", "+"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


class ICUECollector:
    """Read iCUE CSV logs and config-backed coverage metadata."""

    STATIC_DOMAINS = ["device", "fan", "pump", "temperature", "battery"]

    def __init__(self, config: Optional[Dict] = None) -> None:
        cfg = dict(config or {})
        self.source = "icue"
        self.enabled = bool(cfg.get("enabled", False))
        self.exe_path = str(cfg.get("exe_path", "") or "")
        self.csv_path = str(cfg.get("csv_path", "") or "")
        self.csv_dir = str(cfg.get("csv_dir", "") or "")
        self.session_output_dir = str(cfg.get("session_output_dir", "") or "")
        home = Path.home()
        self.discovery_files = [
            Path(cfg.get("user_props_path", home / "AppData/Roaming/Corsair/CUE5/sensors/UserProps")),
            Path(cfg.get("config_path", home / "AppData/Roaming/Corsair/CUE5/config.cuecfg")),
            Path(cfg.get("devices_path", home / "AppData/Local/Corsair/CUE5/cache/devices.json")),
        ]
        self._coverage_cache: Optional[Dict] = None

    def probe(self) -> Dict:
        available = bool(self.exe_path) and Path(self.exe_path).is_file()
        config_present = any(path.is_file() for path in self.discovery_files)
        status = "connected" if available or config_present else "not_found"
        message = "collector executable or config found" if status == "connected" else "collector executable not found"
        return {
            "source": self.source,
            "enabled": self.enabled,
            "available": status == "connected",
            "path": self.exe_path,
            "status": status if self.enabled else "disabled",
            "message": message if self.enabled else "collector disabled",
            "timestamp_utc": _utc_now(),
        }

    def start_session(self, run_id: str, session_config: Optional[Dict] = None) -> Dict:
        if session_config:
            self.__init__({**self.__dict__, **session_config})
        return {
            "source": self.source,
            "run_id": run_id,
            "status": self._current_status(),
            "timestamp_utc": _utc_now(),
        }

    def read_latest(self) -> Dict:
        sample = {
            "source": self.source,
            "timestamp_utc": _utc_now(),
            "status": self._current_status(),
            "raw_log_path": "",
            "metrics": {},
            "coverage": self.describe_coverage(),
        }

        if not self.enabled:
            sample["status"] = "disabled"
            return sample

        csv_file = self._resolve_csv_file()
        if csv_file is not None:
            parsed = self._parse_csv(csv_file)
            if parsed is not None:
                sample["status"] = "connected"
                sample["raw_log_path"] = str(csv_file)
                sample["metrics"] = parsed["metrics"]
                sample["coverage"] = parsed["coverage"]
                self._coverage_cache = parsed["coverage"]
                return sample
            sample["status"] = "parse_error"
            sample["raw_log_path"] = str(csv_file)
            return sample

        if any(path.is_file() for path in self.discovery_files):
            sample["status"] = "log_missing"
            return sample

        if Path(self.exe_path).is_file():
            sample["status"] = "log_missing"
        else:
            sample["status"] = "not_found"
        return sample

    def stop_session(self) -> Dict:
        return {
            "source": self.source,
            "status": self._current_status(),
            "coverage": self.describe_coverage(),
            "timestamp_utc": _utc_now(),
        }

    def describe_coverage(self) -> Dict:
        if self._coverage_cache is not None:
            return self._coverage_cache

        raw_metric_names = self._discover_metric_names()
        domains = sorted({self._metric_domain(name) for name in raw_metric_names if self._metric_domain(name)})
        if not domains:
            domains = list(self.STATIC_DOMAINS)
        metric_keys = sorted(
            {
                key
                for name in raw_metric_names
                for key in [self._normalized_metric_key(name)]
                if key is not None
            }
        )
        self._coverage_cache = {
            "source": self.source,
            "domains": domains,
            "metric_keys": metric_keys,
            "raw_metric_names": raw_metric_names,
            "notes": "Coverage is derived from iCUE sensor configuration and cached device metadata.",
        }
        return self._coverage_cache

    def _current_status(self) -> str:
        if not self.enabled:
            return "disabled"
        if Path(self.exe_path).is_file():
            return "connected"
        if any(path.is_file() for path in self.discovery_files):
            return "connected"
        return "not_found"

    def _resolve_csv_file(self) -> Optional[Path]:
        explicit_file = Path(self.csv_path) if self.csv_path else None
        if explicit_file and explicit_file.is_file():
            return explicit_file

        candidate_dirs = []
        for raw_dir in (self.csv_dir,):
            if raw_dir:
                path = Path(raw_dir)
                if path.is_dir():
                    candidate_dirs.append(path)

        home = Path.home()
        candidate_dirs.extend(
            [
                home / "Documents",
                home / "Documents/Corsair",
                home / "Documents/CORSAIR",
                home / "AppData/Local/Corsair/CUE5",
                home / "AppData/Roaming/Corsair/CUE5",
                Path.cwd(),
            ]
        )

        patterns = ("*iCUE*.csv", "*icue*.csv", "*corsair*.csv")
        candidates: List[Path] = []
        for directory in candidate_dirs:
            if not directory.is_dir():
                continue
            for pattern in patterns:
                candidates.extend(directory.glob(pattern))
                candidates.extend(directory.glob(f"**/{pattern}"))

        candidates = [item for item in candidates if item.is_file()]
        if not candidates:
            return None
        candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
        return candidates[0]

    def _parse_csv(self, csv_file: Path) -> Optional[Dict]:
        try:
            with csv_file.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                last_row = next(deque(reader, maxlen=1), None)
                headers = list(reader.fieldnames or [])
        except OSError:
            return None

        if not last_row:
            return None

        metrics = {}
        metrics["cpu_proxy"] = self._extract_metric(headers, last_row, ("cpu load", "load", "cpu usage"), ("gpu",))
        metrics["gpu_series"] = self._extract_metric(
            headers,
            last_row,
            ("gpu load", "framebufferload", "videoengineload", "graphics load"),
            (),
        )
        metrics["cpu_temp_c"] = self._extract_metric(
            headers,
            last_row,
            ("temperaturepackage", "cpu package", "temperature package", "cpu temp"),
            (),
        )
        metrics["power_w"] = self._extract_metric(headers, last_row, ("power", "package power"), ())
        metrics["fan_rpm"] = self._average_headers(headers, last_row, ("fan",), ())
        metrics["pump_rpm"] = self._average_headers(headers, last_row, ("pump",), ())
        metrics["coolant_temp_c"] = self._extract_metric(headers, last_row, ("coolant", "liquid temp"), ())
        metrics["device_battery_level"] = self._extract_metric(headers, last_row, ("battery level", "dev-battery-level"), ())

        coverage = {
            "source": self.source,
            "domains": sorted({self._metric_domain(header) for header in headers if self._metric_domain(header)}),
            "metric_keys": sorted(
                {
                    key
                    for header in headers
                    for key in [self._normalized_metric_key(header)]
                    if key is not None
                }
            ),
            "raw_metric_names": headers,
            "notes": "Derived from the latest iCUE CSV export.",
        }
        return {"metrics": metrics, "coverage": coverage}

    @staticmethod
    def _extract_metric(
        headers: Iterable[str], row: Dict[str, object], includes: Iterable[str], excludes: Iterable[str]
    ) -> Optional[float]:
        include_tokens = [token.lower() for token in includes]
        exclude_tokens = [token.lower() for token in excludes]
        for header in headers:
            header_l = str(header).lower()
            if not any(token in header_l for token in include_tokens):
                continue
            if any(token in header_l for token in exclude_tokens):
                continue
            value = _safe_float(row.get(header))
            if value is not None:
                return value
        return None

    @staticmethod
    def _average_headers(
        headers: Iterable[str], row: Dict[str, object], includes: Iterable[str], excludes: Iterable[str]
    ) -> Optional[float]:
        include_tokens = [token.lower() for token in includes]
        exclude_tokens = [token.lower() for token in excludes]
        values = []
        for header in headers:
            header_l = str(header).lower()
            if not any(token in header_l for token in include_tokens):
                continue
            if any(token in header_l for token in exclude_tokens):
                continue
            value = _safe_float(row.get(header))
            if value is not None:
                values.append(value)
        if not values:
            return None
        return sum(values) / float(len(values))

    def _discover_metric_names(self) -> List[str]:
        tokens = set()
        for path in self.discovery_files:
            if not path.is_file():
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            for token in re.findall(r"[A-Za-z][A-Za-z0-9_-]{2,}", text):
                domain = self._metric_domain(token)
                normalized = self._normalized_metric_key(token)
                if domain or normalized or token.lower() in {"fan", "pump", "power-supply"}:
                    tokens.add(token)
        return sorted(tokens)

    @staticmethod
    def _metric_domain(name: str) -> Optional[str]:
        token = str(name).lower()
        if "battery" in token:
            return "battery"
        if "pump" in token:
            return "pump"
        if "fan" in token:
            return "fan"
        if "coolant" in token or "temp" in token or "temperature" in token:
            return "temperature"
        if "power" in token or "psu" in token:
            return "psu" if "psu" in token or "power-supply" in token else "power"
        if "voltage" in token:
            return "voltage"
        if "gpu" in token or "framebuffer" in token or "videoengine" in token:
            return "gpu"
        if "memory" in token:
            return "memory"
        if "load" in token or "cpu" in token:
            return "cpu"
        if "device" in token:
            return "device"
        return None

    @staticmethod
    def _normalized_metric_key(name: str) -> Optional[str]:
        token = str(name).lower()
        if "battery level" in token or "dev-battery-level" in token:
            return "device_battery_level"
        if "coolant" in token or "liquid temp" in token:
            return "coolant_temp_c"
        if "pump" in token:
            return "pump_rpm"
        if "fan" in token:
            return "fan_rpm"
        if "temperaturepackage" in token or "cpu package" in token or "temperature package" in token:
            return "cpu_temp_c"
        if "power" in token:
            return "power_w"
        if "framebufferload" in token or "videoengineload" in token or "gpu load" in token:
            return "gpu_series"
        if "memoryload" in token:
            return "memory_series"
        if "load" in token:
            return "cpu_proxy"
        return None


if __name__ == "__main__":
    collector = ICUECollector({"enabled": True})
    print(json.dumps(collector.read_latest(), indent=2))
