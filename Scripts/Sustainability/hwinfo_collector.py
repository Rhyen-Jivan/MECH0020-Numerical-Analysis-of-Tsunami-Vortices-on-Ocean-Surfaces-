#!/usr/bin/env python3
"""HWiNFO collector runtime.

Primary path: live shared-memory sensor transport.
Fallback path: explicit CSV logs only from configured runtime directories.
"""

from __future__ import annotations

import csv
import ctypes
import ctypes.wintypes as wintypes
import json
import os
import re
import struct
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional


def _utc_now() -> str:
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


def _decode_c_string(raw: bytes) -> str:
    text = raw.split(b"\x00", 1)[0].decode("utf-8", errors="ignore").strip()
    return re.sub(r"\s+", " ", text)


def _sanitize_metric_token(text: str) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "_", str(text).strip().lower())
    token = re.sub(r"_+", "_", token).strip("_")
    return token or "metric"


class HWiNFOCollector:
    """Read HWiNFO shared-memory sensor data and explicit CSV fallbacks."""

    SHARED_MEMORY_NAME = "Global\\HWiNFO_SENS_SM2"
    SHARED_MUTEX_NAME = "Global\\HWiNFO_SM2_MUTEX"
    FILE_MAP_READ = 0x0004
    SYNCHRONIZE = 0x00100000
    MUTEX_MODIFY_STATE = 0x0001
    WAIT_OBJECT_0 = 0x00000000
    WAIT_ABANDONED = 0x00000080
    WAIT_TIMEOUT_MS = 50
    _kernel32_cached = None

    HEADER_FMT = "<IIIqIIIIII"
    SENSOR_FMT = "<II128s128s"
    READING_FMT = "<III128s128s16sdddd"
    HEADER_SIZE = struct.calcsize(HEADER_FMT)
    SENSOR_SIZE = struct.calcsize(SENSOR_FMT)
    READING_SIZE = struct.calcsize(READING_FMT)

    STATIC_DOMAINS = ["cpu", "gpu", "memory", "temperature", "power", "voltage", "fan", "pump", "clock"]
    AVAILABLE_STATUSES = {"shared_memory_connected", "csv_fallback", "csv_target_mismatch"}
    METRIC_DEFS = {
        "cpu_proxy": {"name": "CPU Usage", "unit": "%", "domain": "cpu"},
        "gpu_series": {"name": "GPU Usage", "unit": "%", "domain": "gpu"},
        "memory_series": {"name": "Memory Usage", "unit": "%", "domain": "memory"},
        "cpu_voltage_v": {"name": "CPU Voltage", "unit": "V", "domain": "voltage"},
        "gpu_voltage_v": {"name": "GPU Voltage", "unit": "V", "domain": "voltage"},
        "memory_voltage_v": {"name": "Memory Voltage", "unit": "V", "domain": "voltage"},
        "cpu_temp_c": {"name": "CPU Package Temperature", "unit": "C", "domain": "temperature"},
        "power_w": {"name": "CPU Package Power", "unit": "W", "domain": "power"},
        "cpu_power_w_hwinfo": {"name": "CPU Package Power", "unit": "W", "domain": "power"},
        "gpu_power_w_hwinfo": {"name": "GPU Power", "unit": "W", "domain": "power"},
        "memory_power_w_or_proxy": {"name": "Memory Power / Demand Proxy", "unit": "", "domain": "power"},
        "system_power_w": {"name": "Total System Power", "unit": "W", "domain": "power"},
        "fan_rpm": {"name": "Fan Speed", "unit": "RPM", "domain": "fan"},
        "pump_rpm": {"name": "Pump Speed", "unit": "RPM", "domain": "pump"},
    }
    CURATED_CSV_INCLUDE_TOKENS = (
        "usage",
        "load",
        "utilization",
        "power",
        "voltage",
        "vid",
        "vddq",
        "temperature",
        "temp",
        "fan",
        "pump",
        "clock",
        "frequency",
    )

    def __init__(self, config: Optional[Dict] = None) -> None:
        cfg = dict(config or {})
        self.source = "hwinfo"
        self.enabled = bool(cfg.get("enabled", False))
        self.exe_path = str(cfg.get("exe_path", "") or "")
        self.transport_mode = str(cfg.get("transport_mode", "auto") or "auto").strip().lower()
        if self.transport_mode not in {"auto", "shared_memory", "csv"}:
            self.transport_mode = "auto"
        self.csv_target_path = str(cfg.get("csv_target_path", "") or "")
        self.csv_target_dir = str(cfg.get("csv_target_dir", "") or "")
        self.csv_path = str(cfg.get("csv_path", "") or "")
        self.csv_dir = str(cfg.get("csv_dir", "") or "")
        self.session_output_dir = str(cfg.get("session_output_dir", "") or "")
        self.shared_memory_blob_path = str(cfg.get("shared_memory_blob_path", "") or "")
        self.sample_interval = max(0.1, float(cfg.get("sample_interval", 0.5) or 0.5))
        self.launch_if_needed = bool(cfg.get("launch_if_needed", True))
        self._coverage_cache: Optional[Dict] = None
        self._last_transport = "none"
        self._shared_memory_seen = False
        self._launched_this_session = False

    @classmethod
    def _kernel32(cls):
        if cls._kernel32_cached is not None:
            return cls._kernel32_cached
        if not hasattr(ctypes, "WinDLL"):
            return None

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenMutexW.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR]
        kernel32.OpenMutexW.restype = wintypes.HANDLE
        kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        kernel32.OpenFileMappingW.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR]
        kernel32.OpenFileMappingW.restype = wintypes.HANDLE
        kernel32.MapViewOfFile.argtypes = [
            wintypes.HANDLE,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            ctypes.c_size_t,
        ]
        kernel32.MapViewOfFile.restype = ctypes.c_void_p
        kernel32.UnmapViewOfFile.argtypes = [ctypes.c_void_p]
        kernel32.UnmapViewOfFile.restype = wintypes.BOOL
        kernel32.ReleaseMutex.argtypes = [wintypes.HANDLE]
        kernel32.ReleaseMutex.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        cls._kernel32_cached = kernel32
        return kernel32

    @classmethod
    def win32_binding_summary(cls) -> Dict[str, object]:
        summary = {
            "shared_memory_name": cls.SHARED_MEMORY_NAME,
            "shared_mutex_name": cls.SHARED_MUTEX_NAME,
            "bindings_configured": False,
            "functions": {},
        }
        kernel32 = cls._kernel32()
        if kernel32 is None:
            summary["binding_error"] = "kernel32 bindings unavailable"
            return summary

        summary["bindings_configured"] = True
        for name in (
            "OpenMutexW",
            "WaitForSingleObject",
            "OpenFileMappingW",
            "MapViewOfFile",
            "UnmapViewOfFile",
            "ReleaseMutex",
            "CloseHandle",
        ):
            fn = getattr(kernel32, name, None)
            summary["functions"][name] = {
                "argtypes_configured": bool(fn is not None and getattr(fn, "argtypes", None) is not None),
                "restype_configured": bool(fn is not None and getattr(fn, "restype", None) is not None),
            }
        return summary

    def probe(self) -> Dict:
        try:
            sample = self.read_latest()
        except Exception as exc:  # pragma: no cover - defensive probe guard
            sample = {
                "status": "parse_error",
                "status_reason": str(exc),
                "message": str(exc),
                "transport": "none",
                "probe_details": {},
            }
        return {
            "source": self.source,
            "enabled": self.enabled,
            "available": sample.get("status") in self.AVAILABLE_STATUSES,
            "path": self.exe_path,
            "status": sample.get("status", "disabled"),
            "message": sample.get("status_reason", sample.get("message", "")),
            "transport": sample.get("transport", "none"),
            "probe_details": sample.get("probe_details", {}),
            "timestamp_utc": _utc_now(),
        }

    def start_session(self, run_id: str, session_config: Optional[Dict] = None) -> Dict:
        if session_config:
            merged = {
                "enabled": self.enabled,
                "exe_path": self.exe_path,
                "csv_target_path": self.csv_target_path,
                "csv_target_dir": self.csv_target_dir,
                "csv_path": self.csv_path,
                "csv_dir": self.csv_dir,
                "session_output_dir": self.session_output_dir,
                "shared_memory_blob_path": self.shared_memory_blob_path,
                "sample_interval": self.sample_interval,
                "launch_if_needed": self.launch_if_needed,
            }
            merged.update(session_config)
            self.__init__(merged)
        self._prepare_runtime_environment()
        probe = self.probe()
        return {
            "source": self.source,
            "run_id": run_id,
            "status": probe.get("status", "disabled"),
            "transport": probe.get("transport", "none"),
            "timestamp_utc": _utc_now(),
        }

    def read_latest(self) -> Dict:
        sample = {
            "source": self.source,
            "timestamp_utc": _utc_now(),
            "status": self._current_status(),
            "status_reason": "",
            "transport": "none",
            "message": "",
            "raw_log_path": "",
            "metrics": {},
            "coverage": self.describe_coverage(),
            "metric_catalog": [],
            "probe_details": {},
        }
        if not self.enabled:
            sample["status"] = "disabled"
            sample["status_reason"] = "collector disabled"
            sample["message"] = sample["status_reason"]
            return sample

        probe_details = self._prepare_runtime_environment()
        sample["probe_details"] = probe_details

        payload = None
        if self.transport_mode in {"auto", "shared_memory"} and probe_details.get("shared_memory_available", False):
            try:
                payload = self._read_shared_memory_sample()
            except Exception as exc:  # pragma: no cover - defensive bridge guard
                payload = {"status": "shared_memory_error", "message": str(exc)}

        if payload and payload.get("metrics"):
            sample["status"] = "shared_memory_connected"
            sample["status_reason"] = "shared memory connected"
            sample["transport"] = "shared_memory"
            sample["message"] = sample["status_reason"]
            sample["metrics"] = payload["metrics"]
            sample["coverage"] = payload["coverage"]
            sample["metric_catalog"] = payload["metric_catalog"]
            self._coverage_cache = payload["coverage"]
            self._shared_memory_seen = True
            self._last_transport = "shared_memory"
            return sample

        csv_file = None
        if self.transport_mode in {"auto", "csv"} and probe_details.get("csv_available", False):
            csv_file = Path(str(probe_details.get("csv_path", "") or ""))
        if csv_file is not None and csv_file.is_file():
            parsed = self._parse_csv(csv_file)
            if parsed is not None:
                if probe_details.get("csv_target_mismatch", False):
                    sample["status"] = "csv_target_mismatch"
                else:
                    sample["status"] = "csv_fallback"
                sample["status_reason"] = self._csv_fallback_reason(probe_details)
                sample["transport"] = "csv"
                sample["message"] = sample["status_reason"]
                sample["raw_log_path"] = str(csv_file)
                sample["metrics"] = parsed["metrics"]
                sample["coverage"] = parsed["coverage"]
                sample["metric_catalog"] = parsed["metric_catalog"]
                self._coverage_cache = parsed["coverage"]
                self._last_transport = "csv"
                return sample
            sample["status"] = "parse_error"
            sample["status_reason"] = "configured CSV fallback could not be parsed"
            sample["message"] = sample["status_reason"]
            sample["raw_log_path"] = str(csv_file)
            return sample

        if payload and payload.get("status") == "shared_memory_error":
            sample["status"] = "parse_error"
            sample["status_reason"] = payload.get("message", "shared memory parse failed")
            sample["message"] = sample["status_reason"]
            return sample

        sample["status"], sample["status_reason"] = self._no_transport_status(probe_details)
        sample["message"] = sample["status_reason"]
        self._last_transport = "none"
        return sample

    def stop_session(self) -> Dict:
        probe = self.probe()
        return {
            "source": self.source,
            "status": probe.get("status", "disabled"),
            "transport": probe.get("transport", "none"),
            "coverage": self.describe_coverage(),
            "timestamp_utc": _utc_now(),
        }

    def describe_coverage(self) -> Dict:
        if self._coverage_cache is not None:
            return self._coverage_cache
        return {
            "source": self.source,
            "domains": list(self.STATIC_DOMAINS),
            "metric_keys": sorted(self.METRIC_DEFS.keys()),
            "raw_metric_names": [],
            "notes": "Coverage defaults to canonical HWiNFO runtime metrics until live shared memory or explicit CSV data is observed.",
        }

    def _current_status(self) -> str:
        if not self.enabled:
            return "disabled"
        if Path(self.exe_path).is_file() or self._configured_shared_memory_blob_exists():
            return "csv_missing"
        return "not_found"

    def _configured_shared_memory_blob_exists(self) -> bool:
        return bool(self.shared_memory_blob_path and Path(self.shared_memory_blob_path).is_file())

    def _resolve_target_csv_file(self) -> Optional[Path]:
        explicit_target = Path(self.csv_target_path) if self.csv_target_path else None
        if explicit_target and explicit_target.is_file():
            return explicit_target
        if self.csv_target_dir:
            target_dir = Path(self.csv_target_dir)
            if target_dir.is_dir():
                return self._latest_csv_in_dir(target_dir)
        return None

    def _resolve_observed_csv_file(self) -> Optional[Path]:
        explicit_file = Path(self.csv_path) if self.csv_path else None
        if explicit_file and explicit_file.is_file():
            return explicit_file
        candidate_dirs: List[Path] = []
        for raw_dir in (self.csv_dir,):
            if raw_dir:
                path = Path(raw_dir)
                if path.is_dir():
                    candidate_dirs.append(path)
        for directory in candidate_dirs:
            candidate = self._latest_csv_in_dir(directory)
            if candidate is not None:
                return candidate
        return None

    def _latest_csv_in_dir(self, directory: Path) -> Optional[Path]:
        patterns = ("*hwinfo*.csv", "*HWiNFO*.csv", "*.csv")
        candidates: List[Path] = []
        for pattern in patterns:
            candidates.extend(directory.glob(pattern))
        candidates = [item for item in candidates if item.is_file()]
        if not candidates:
            return None
        candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
        return candidates[0]

    def _is_csv_fresh(self, csv_file: Optional[Path], is_explicit: bool) -> bool:
        if csv_file is None or not csv_file.is_file():
            return False
        if is_explicit:
            return True
        age_seconds = max(0.0, time.time() - csv_file.stat().st_mtime)
        return age_seconds <= 30 * 60

    def _csv_probe_details(
        self,
        target_csv_file: Optional[Path],
        observed_csv_file: Optional[Path],
    ) -> Dict[str, object]:
        target_is_explicit = bool(
            self.csv_target_path
            and target_csv_file
            and Path(self.csv_target_path).resolve() == target_csv_file.resolve()
        )
        observed_is_explicit = bool(
            self.csv_path
            and observed_csv_file
            and Path(self.csv_path).resolve() == observed_csv_file.resolve()
        )
        target_available = self._is_csv_fresh(target_csv_file, target_is_explicit)
        observed_available = self._is_csv_fresh(observed_csv_file, observed_is_explicit)
        target_configured = bool(self.csv_target_path or self.csv_target_dir)
        mismatch = False
        if target_configured and observed_available and observed_csv_file is not None and not target_available:
            mismatch = True
            if target_csv_file is not None:
                mismatch = target_csv_file.resolve() != observed_csv_file.resolve()
        active_csv = target_csv_file if target_available else observed_csv_file
        csv_available = bool(target_available or observed_available)
        return {
            "csv_target_path": self.csv_target_path,
            "csv_target_dir": self.csv_target_dir,
            "csv_target_resolved_path": str(target_csv_file) if target_csv_file is not None else "",
            "csv_target_available": bool(target_available),
            "csv_target_configured": bool(target_configured),
            "csv_observed_path": str(observed_csv_file) if observed_csv_file is not None else "",
            "csv_observed_available": bool(observed_available),
            "csv_path": str(active_csv) if active_csv is not None else "",
            "csv_dir": str(active_csv.parent) if active_csv is not None else "",
            "csv_available": bool(csv_available),
            "csv_target_mismatch": bool(mismatch),
        }

    def _prepare_runtime_environment(self) -> Dict[str, object]:
        exe_found = Path(self.exe_path).is_file()
        ini_path, ini_shared_memory_enabled = self._resolve_ini_shared_memory_state()
        ini_updated = self._try_enable_shared_memory_in_ini(ini_path, ini_shared_memory_enabled)
        ini_csv_target = self._inspect_ini_csv_target(ini_path)
        ini_csv_target_sync = self._try_sync_csv_target_in_ini(ini_path, ini_csv_target)
        process_running = self._is_hwinfo_process_running()
        launched = False
        if self.launch_if_needed and exe_found and not process_running:
            launched = self._launch_hwinfo_process()
            if launched:
                time.sleep(min(self.sample_interval, 1.5))
                process_running = self._is_hwinfo_process_running()

        shared_memory_blob = None
        shared_memory_probe = self._shared_memory_probe_details_base()
        try:
            shared_memory_blob, shared_memory_probe = self._read_shared_memory_blob(return_details=True)
        except Exception as exc:
            shared_memory_probe["shared_memory_status_detail"] = str(exc)
            shared_memory_probe["shared_memory_exception"] = str(exc)

        target_csv_file = self._resolve_target_csv_file()
        observed_csv_file = self._resolve_observed_csv_file()
        csv_probe = self._csv_probe_details(target_csv_file, observed_csv_file)
        if csv_probe.get("csv_target_mismatch", False):
            active_csv = observed_csv_file
        elif csv_probe.get("csv_target_available", False):
            active_csv = target_csv_file
        else:
            active_csv = observed_csv_file

        return {
            "exe_found": exe_found,
            "process_running": process_running,
            "launched_process": bool(launched or self._launched_this_session),
            "transport_mode": self.transport_mode,
            "ini_path": ini_path,
            "ini_shared_memory_enabled": ini_shared_memory_enabled,
            "ini_shared_memory_state": self._ini_shared_memory_state_text(ini_shared_memory_enabled, ini_updated),
            "ini_updated": bool(ini_updated),
            "ini_csv_target_key": ini_csv_target.get("key", ""),
            "ini_csv_target_value": ini_csv_target.get("value", ""),
            "csv_target_sync_status": ini_csv_target_sync.get("status", "not_configured"),
            "shared_memory_previously_connected": bool(self._shared_memory_seen),
            "csv_path": str(active_csv) if active_csv is not None else "",
            "csv_dir": str(active_csv.parent) if active_csv is not None else "",
            "shared_memory_available": bool(shared_memory_blob is not None),
            **shared_memory_probe,
            **csv_probe,
        }

    def _csv_fallback_reason(self, probe_details: Dict[str, object]) -> str:
        if bool(probe_details.get("csv_target_mismatch", False)):
            return "CSV target configured, but HWiNFO is logging elsewhere"
        if bool(probe_details.get("shared_memory_previously_connected", False)):
            return "shared memory expired; CSV fallback active"
        if probe_details.get("ini_shared_memory_enabled") is False:
            return "shared memory disabled; CSV fallback active"
        return "shared memory unavailable; CSV fallback active"

    def _no_transport_status(self, probe_details: Dict[str, object]) -> tuple[str, str]:
        shm_detail = str(probe_details.get("shared_memory_status_detail", "") or "").strip()
        if not bool(probe_details.get("exe_found", False)) and not self._configured_shared_memory_blob_exists():
            return "not_found", "collector executable not found"
        if probe_details.get("ini_shared_memory_enabled") is False:
            return "shared_memory_disabled", "shared memory disabled; CSV logging not active"
        if bool(probe_details.get("shared_memory_previously_connected", False)):
            return "shared_memory_expired", "shared memory expired; CSV logging not active"
        if bool(probe_details.get("csv_target_configured", False)):
            if shm_detail:
                return "csv_missing", f"shared memory unavailable; configured CSV target is not active ({shm_detail})"
            return "csv_missing", "shared memory unavailable; configured CSV target is not active"
        if shm_detail:
            return "csv_missing", f"shared memory unavailable; CSV logging not active ({shm_detail})"
        return "csv_missing", "shared memory unavailable; CSV logging not active"

    def _resolve_ini_shared_memory_state(self) -> tuple[str, Optional[bool]]:
        for candidate in self._hwinfo_ini_candidates():
            if not candidate.is_file():
                continue
            text = self._read_text_file(candidate)
            if text is None:
                continue
            match = re.search(r"(?im)^\s*SensorsSM\s*=\s*(\d+)\s*$", text)
            if match:
                return str(candidate), match.group(1).strip() == "1"
            return str(candidate), None
        return "", None

    def _try_enable_shared_memory_in_ini(self, ini_path: str, ini_shared_memory_enabled: Optional[bool]) -> bool:
        if ini_shared_memory_enabled is True or not ini_path:
            return False
        path = Path(ini_path)
        if not path.is_file() or not os.access(path, os.W_OK):
            return False
        text = self._read_text_file(path)
        if text is None:
            return False
        updated = False
        if re.search(r"(?im)^\s*SensorsSM\s*=\s*\d+\s*$", text):
            text_new = re.sub(r"(?im)^(\s*SensorsSM\s*=\s*)\d+\s*$", r"\g<1>1", text)
            updated = text_new != text
            text = text_new
        else:
            line_sep = "\r\n" if "\r\n" in text else "\n"
            text = text.rstrip() + f"{line_sep}SensorsSM=1{line_sep}"
            updated = True
        if not updated:
            return False
        try:
            path.write_text(text, encoding="utf-8")
            return True
        except OSError:
            return False

    @staticmethod
    def _ini_shared_memory_state_text(flag: Optional[bool], was_updated: bool) -> str:
        if flag is True and not was_updated:
            return "enabled"
        if flag is False and was_updated:
            return "enforced_to_enabled"
        if flag is False:
            return "disabled"
        if was_updated:
            return "enforced_to_enabled"
        return "unknown"

    def _inspect_ini_csv_target(self, ini_path: str) -> Dict[str, str]:
        if not ini_path:
            return {"key": "", "value": "", "kind": ""}
        path = Path(ini_path)
        text = self._read_text_file(path)
        if text is None:
            return {"key": "", "value": "", "kind": ""}

        best_match = {"key": "", "value": "", "kind": ""}
        for raw_line in text.splitlines():
            match = re.match(r"^\s*([^=;#][^=]*)=(.*)$", raw_line)
            if not match:
                continue
            key = match.group(1).strip()
            value = match.group(2).strip().strip('"')
            key_lower = key.lower()
            value_lower = value.lower()
            if "log" not in key_lower and "csv" not in key_lower:
                continue
            if not value or value_lower in {"0", "1", "true", "false"}:
                continue
            if value_lower.endswith(".csv"):
                return {"key": key, "value": value, "kind": "file"}
            if ("\\" in value or "/" in value) and not best_match["key"]:
                best_match = {"key": key, "value": value, "kind": "dir"}
        return best_match

    def _try_sync_csv_target_in_ini(self, ini_path: str, ini_csv_target: Dict[str, str]) -> Dict[str, str]:
        target_configured = bool(self.csv_target_path or self.csv_target_dir)
        if not ini_path:
            return {"status": "no_ini"}
        if not target_configured:
            return {"status": "not_configured"}
        if not ini_csv_target.get("key"):
            return {"status": "key_not_found"}

        replacement_value = self._replacement_csv_target_value(ini_csv_target)
        if not replacement_value:
            return {"status": "key_not_supported"}

        path = Path(ini_path)
        if not path.is_file() or not os.access(path, os.W_OK):
            return {"status": "ini_read_only"}

        text = self._read_text_file(path)
        if text is None:
            return {"status": "ini_unreadable"}

        key_pattern = re.escape(ini_csv_target["key"])
        replacement_pattern = rf"(?im)^(\s*{key_pattern}\s*=\s*).*$"
        updated_text, count = re.subn(replacement_pattern, rf"\g<1>{replacement_value}", text)
        if count == 0:
            return {"status": "key_not_found"}
        if updated_text == text:
            return {"status": "already_synced"}

        try:
            path.write_text(updated_text, encoding="utf-8")
            return {"status": "synced"}
        except OSError:
            return {"status": "ini_write_failed"}

    def _replacement_csv_target_value(self, ini_csv_target: Dict[str, str]) -> str:
        kind = str(ini_csv_target.get("kind", "") or "").lower()
        current_value = str(ini_csv_target.get("value", "") or "")
        if kind == "file":
            if self.csv_target_path:
                return self.csv_target_path
            if self.csv_target_dir:
                current_name = Path(current_value).name if current_value else "HWiNFO_LOG.CSV"
                return str(Path(self.csv_target_dir) / current_name)
            return ""
        if kind == "dir":
            if self.csv_target_dir:
                return self.csv_target_dir
            if self.csv_target_path:
                return str(Path(self.csv_target_path).parent)
        return ""

    def _launch_hwinfo_process(self) -> bool:
        exe = Path(self.exe_path)
        if not exe.is_file():
            return False
        try:
            subprocess.Popen(
                [str(exe)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
                **self._hidden_subprocess_kwargs(),
            )
            self._launched_this_session = True
            return True
        except OSError:
            return False

    @staticmethod
    def _read_text_file(path: Path) -> Optional[str]:
        for encoding in ("utf-8-sig", "cp1252", "latin-1"):
            try:
                return path.read_text(encoding=encoding)
            except UnicodeDecodeError:
                continue
            except OSError:
                return None
        return None

    def _hwinfo_ini_candidates(self) -> List[Path]:
        candidates: List[Path] = []
        exe = Path(self.exe_path) if self.exe_path else None
        if exe is not None and str(exe):
            stem = exe.stem or "HWiNFO64"
            candidates.extend(
                [
                    exe.with_suffix(".INI"),
                    exe.parent / f"{stem}.INI",
                    exe.parent / "HWiNFO64.INI",
                    exe.parent / "HWiNFO32.INI",
                    exe.parent / "HWiNFO.INI",
                ]
            )
        appdata = os.getenv("APPDATA", "")
        if appdata:
            appdata_path = Path(appdata)
            candidates.extend(
                [
                    appdata_path / "HWiNFO64.INI",
                    appdata_path / "HWiNFO32.INI",
                    appdata_path / "HWiNFO.INI",
                ]
            )
        unique: List[Path] = []
        seen = set()
        for item in candidates:
            key = str(item).lower()
            if key in seen:
                continue
            seen.add(key)
            unique.append(item)
        return unique

    def _is_hwinfo_process_running(self) -> bool:
        if not hasattr(subprocess, "run"):
            return False
        try:
            proc = subprocess.run(
                ["tasklist", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                check=False,
                timeout=5,
                **self._hidden_subprocess_kwargs(),
            )
        except (OSError, subprocess.SubprocessError):
            return False
        if proc.returncode != 0:
            return False
        names = {"hwinfo64.exe", "hwinfo32.exe", "hwinfo.exe"}
        for line in proc.stdout.splitlines():
            line = line.strip().strip('"')
            if not line:
                continue
            process_name = line.split('","', 1)[0].strip('"').lower()
            if process_name in names:
                return True
        return False

    @staticmethod
    def _hidden_subprocess_kwargs() -> Dict[str, object]:
        if os.name != "nt" or not hasattr(subprocess, "STARTUPINFO"):
            return {}

        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
        startupinfo.wShowWindow = 0
        return {
            "startupinfo": startupinfo,
            "creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0),
        }

    def _read_csv_last_row(self, csv_file: Path) -> Optional[tuple[list[str], list[str]]]:
        for encoding in ("utf-8-sig", "cp1252", "latin-1"):
            try:
                with csv_file.open("r", encoding=encoding, newline="") as handle:
                    reader = csv.reader(handle)
                    headers = next(reader, None)
                    if not headers:
                        return None
                    last_rows = deque(maxlen=1)
                    for row in reader:
                        if self._looks_like_data_row(headers, row):
                            last_rows.append(row)
                    last_row = last_rows[0] if last_rows else None
                    return list(headers), list(last_row or [])
            except UnicodeDecodeError:
                continue
            except OSError:
                return None
        return None

    @staticmethod
    def _looks_like_data_row(headers: list[str], row: list[str]) -> bool:
        if not row or not any(str(cell).strip() for cell in row):
            return False
        first_cell = str(row[0]).strip()
        second_cell = str(row[1]).strip() if len(row) > 1 else ""
        if not first_cell or not second_cell:
            return False
        if first_cell.lower() == str(headers[0]).strip().lower():
            return False
        if len(headers) > 1 and second_cell.lower() == str(headers[1]).strip().lower():
            return False
        return True

    def _read_shared_memory_sample(self) -> Optional[Dict]:
        blob = self._read_shared_memory_blob()
        if not blob:
            return None
        if len(blob) < self.HEADER_SIZE:
            return {"status": "shared_memory_error", "message": "shared memory header is incomplete"}

        header_vals = struct.unpack_from(self.HEADER_FMT, blob, 0)
        _, _, _, _, sensor_offset, sensor_stride, sensor_count, reading_offset, reading_stride, reading_count = header_vals
        if sensor_stride < self.SENSOR_SIZE or reading_stride < self.READING_SIZE:
            return {"status": "shared_memory_error", "message": "shared memory element stride is invalid"}
        if sensor_count > 16384 or reading_count > 65536:
            return {"status": "shared_memory_error", "message": "shared memory element count is unrealistic"}

        sensors: Dict[int, str] = {}
        for index in range(sensor_count):
            offset = sensor_offset + index * sensor_stride
            if offset + self.SENSOR_SIZE > len(blob):
                break
            _, _, sensor_name, sensor_user = struct.unpack_from(self.SENSOR_FMT, blob, offset)
            label = _decode_c_string(sensor_user) or _decode_c_string(sensor_name)
            sensors[int(index)] = label

        readings = []
        for index in range(reading_count):
            offset = reading_offset + index * reading_stride
            if offset + self.READING_SIZE > len(blob):
                break
            _, sensor_index, _, label_orig, label_user, unit_raw, value, _, _, _ = struct.unpack_from(self.READING_FMT, blob, offset)
            if value != value:
                continue
            readings.append(
                {
                    "sensor": sensors.get(int(sensor_index), ""),
                    "name": _decode_c_string(label_user) or _decode_c_string(label_orig),
                    "unit": _decode_c_string(unit_raw),
                    "value": float(value),
                }
            )

        if not readings:
            return None
        matched = self._match_metrics(readings)
        coverage = self._coverage_from_readings(readings, matched["metrics"])
        return {
            "metrics": dict(matched["metrics"]),
            "coverage": coverage,
            "metric_catalog": list(matched["metric_catalog"]),
        }

    def _shared_memory_probe_details_base(self) -> Dict[str, object]:
        binding_summary = self.win32_binding_summary()
        return {
            "shared_memory_name": self.SHARED_MEMORY_NAME,
            "shared_memory_mutex_name": self.SHARED_MUTEX_NAME,
            "shared_memory_binding_configured": bool(binding_summary.get("bindings_configured", False)),
            "shared_memory_source": "none",
            "shared_memory_mutex_opened": False,
            "shared_memory_map_opened": False,
            "shared_memory_view_opened": False,
            "shared_memory_wait_result": -1,
            "shared_memory_status_detail": "shared memory not checked",
            "shared_memory_last_error": 0,
        }

    def _read_shared_memory_blob(self, return_details: bool = False):
        details = self._shared_memory_probe_details_base()
        blob = None
        if self.shared_memory_blob_path:
            blob_path = Path(self.shared_memory_blob_path)
            if blob_path.is_file():
                details["shared_memory_source"] = "blob_fixture"
                details["shared_memory_status_detail"] = "blob fixture loaded"
                blob = blob_path.read_bytes()
                if return_details:
                    return blob, details
                return blob
        if not self.enabled or not hasattr(ctypes, "WinDLL"):
            details["shared_memory_status_detail"] = "Win32 shared-memory bindings unavailable"
            if return_details:
                return None, details
            return None

        kernel32 = self._kernel32()
        if kernel32 is None:
            details["shared_memory_status_detail"] = "kernel32 bindings unavailable"
            if return_details:
                return None, details
            return None
        mutex_handle = None
        mapping_handle = None
        view_ptr = None
        acquired_mutex = False
        try:
            try:
                mutex_handle = kernel32.OpenMutexW(self.SYNCHRONIZE | self.MUTEX_MODIFY_STATE, False, self.SHARED_MUTEX_NAME)
            except Exception:
                mutex_handle = None
            details["shared_memory_mutex_opened"] = bool(mutex_handle)
            if mutex_handle:
                wait_result = kernel32.WaitForSingleObject(mutex_handle, self.WAIT_TIMEOUT_MS)
                details["shared_memory_wait_result"] = int(wait_result)
                if wait_result not in (self.WAIT_OBJECT_0, self.WAIT_ABANDONED):
                    details["shared_memory_status_detail"] = "shared memory mutex wait timed out"
                    details["shared_memory_last_error"] = int(ctypes.get_last_error())
                    if return_details:
                        return None, details
                    return None
                acquired_mutex = True

            mapping_handle = kernel32.OpenFileMappingW(self.FILE_MAP_READ, False, self.SHARED_MEMORY_NAME)
            details["shared_memory_map_opened"] = bool(mapping_handle)
            if not mapping_handle:
                details["shared_memory_status_detail"] = "shared memory mapping not found"
                details["shared_memory_last_error"] = int(ctypes.get_last_error())
                if return_details:
                    return None, details
                return None
            view_ptr = kernel32.MapViewOfFile(mapping_handle, self.FILE_MAP_READ, 0, 0, 0)
            details["shared_memory_view_opened"] = bool(view_ptr)
            if not view_ptr:
                details["shared_memory_status_detail"] = "shared memory view could not be opened"
                details["shared_memory_last_error"] = int(ctypes.get_last_error())
                if return_details:
                    return None, details
                return None

            header_blob = ctypes.string_at(view_ptr, self.HEADER_SIZE)
            header_vals = struct.unpack_from(self.HEADER_FMT, header_blob, 0)
            _, _, _, _, sensor_offset, sensor_stride, sensor_count, reading_offset, reading_stride, reading_count = header_vals
            required_bytes = max(
                self.HEADER_SIZE,
                sensor_offset + max(sensor_stride, self.SENSOR_SIZE) * max(sensor_count, 0),
                reading_offset + max(reading_stride, self.READING_SIZE) * max(reading_count, 0),
            )
            required_bytes = min(required_bytes, 8 * 1024 * 1024)
            blob = ctypes.string_at(view_ptr, required_bytes)
            details["shared_memory_source"] = "live"
            details["shared_memory_status_detail"] = "shared memory mapping opened"
            if return_details:
                return blob, details
            return blob
        finally:
            if view_ptr:
                try:
                    kernel32.UnmapViewOfFile(view_ptr)
                except Exception:
                    pass
            if mapping_handle:
                try:
                    kernel32.CloseHandle(mapping_handle)
                except Exception:
                    pass
            if mutex_handle and acquired_mutex:
                try:
                    kernel32.ReleaseMutex(mutex_handle)
                except Exception:
                    pass
            if mutex_handle:
                try:
                    kernel32.CloseHandle(mutex_handle)
                except Exception:
                    pass

    def _pick_value(
        self,
        readings: Iterable[Dict[str, object]],
        include_tokens: Iterable[str],
        exclude_tokens: Iterable[str] = (),
        required_units: Iterable[str] = (),
    ) -> Optional[float]:
        include_tokens = [token.lower() for token in include_tokens]
        exclude_tokens = [token.lower() for token in exclude_tokens]
        required_units = [token.lower() for token in required_units]
        for reading in readings:
            combined = f"{reading.get('sensor', '')} {reading.get('name', '')}".lower()
            unit = str(reading.get("unit", "")).lower()
            if not any(token in combined for token in include_tokens):
                continue
            if any(token in combined for token in exclude_tokens):
                continue
            if required_units and not any(token in unit for token in required_units):
                continue
            value = _safe_float(reading.get("value"))
            if value is not None:
                return value
        return None

    def _average_values(
        self,
        readings: Iterable[Dict[str, object]],
        include_tokens: Iterable[str],
        exclude_tokens: Iterable[str] = (),
        required_units: Iterable[str] = (),
    ) -> Optional[float]:
        include_tokens = [token.lower() for token in include_tokens]
        exclude_tokens = [token.lower() for token in exclude_tokens]
        required_units = [token.lower() for token in required_units]
        values: List[float] = []
        for reading in readings:
            combined = f"{reading.get('sensor', '')} {reading.get('name', '')}".lower()
            unit = str(reading.get("unit", "")).lower()
            if not any(token in combined for token in include_tokens):
                continue
            if any(token in combined for token in exclude_tokens):
                continue
            if required_units and not any(token in unit for token in required_units):
                continue
            value = _safe_float(reading.get("value"))
            if value is not None:
                values.append(value)
        if not values:
            return None
        return sum(values) / float(len(values))

    def _coverage_from_readings(self, readings: List[Dict[str, object]], metrics: Dict[str, Optional[float]]) -> Dict:
        domains = sorted({self.METRIC_DEFS[key]["domain"] for key, value in metrics.items() if value is not None})
        if not domains:
            domains = list(self.STATIC_DOMAINS)
        raw_names = sorted({str(reading.get("name", "")) for reading in readings if str(reading.get("name", ""))})
        metric_keys = sorted([key for key, value in metrics.items() if value is not None])
        return {
            "source": self.source,
            "domains": domains,
            "metric_keys": metric_keys,
            "raw_metric_names": raw_names,
            "notes": f"Derived from the HWiNFO {self._last_transport} transport.",
        }

    def _parse_csv(self, csv_file: Path) -> Optional[Dict]:
        parsed_rows = self._read_csv_last_row(csv_file)
        if parsed_rows is None:
            return None
        headers, last_row = parsed_rows
        if not headers or not last_row:
            return None
        padded_row = list(last_row) + [""] * max(0, len(headers) - len(last_row))
        readings = [
            {
                "name": header,
                "sensor": "",
                "unit": self._infer_unit(header),
                "value": _safe_float(padded_row[index] if index < len(padded_row) else ""),
            }
            for index, header in enumerate(headers)
        ]
        matched = self._match_metrics(readings)
        curated_csv = self._curated_csv_metrics(headers, padded_row)
        metrics = dict(matched["metrics"])
        metrics.update(curated_csv["metrics"])
        metric_catalog = list(matched["metric_catalog"]) + list(curated_csv["metric_catalog"])
        coverage = self._coverage_from_readings(readings, matched["metrics"])
        return {"metrics": metrics, "coverage": coverage, "metric_catalog": metric_catalog}

    def _match_metrics(self, readings: List[Dict[str, object]]) -> Dict[str, object]:
        cpu_voltage = self._average_values(
            readings,
            ("core vids", "cpu voltage", "vcore", " vid"),
            ("gpu", "memory", "video", "uncore", "sa vid"),
            ("v",),
        )
        gpu_voltage = self._pick_value(
            readings,
            ("gpu core voltage", "gpu voltage", "igpu vid", "gpu vid"),
            ("memory",),
            ("v",),
        )
        memory_voltage = self._pick_value(
            readings,
            ("vddq tx voltage", "dram voltage", "memory voltage", "ram voltage", "vddq"),
            ("gpu",),
            ("v",),
        )
        memory_power_direct = self._pick_value(
            readings,
            ("dram power", "dimm power", "memory power", "ram power", "ddr power"),
            ("gpu", "cpu"),
            ("w",),
        )
        metrics = {
            "cpu_proxy": self._pick_value(readings, ("total cpu", "cpu total", "total cpu usage", "cpu usage"), ("gpu", "memory")),
            "gpu_series": self._pick_value(readings, ("gpu core load", "gpu total usage", "gpu total", "gpu load", "gpu usage", "gpu utilization"), ("memory", "video engine")),
            "memory_series": self._pick_value(readings, ("physical memory load", "memory usage", "memory load"), ("gpu",), ("%", "percent")),
            "cpu_voltage_v": cpu_voltage,
            "gpu_voltage_v": gpu_voltage,
            "memory_voltage_v": memory_voltage,
            "cpu_temp_c": self._pick_value(readings, ("cpu package", "package temperature", "cpu temp"), (), ("c", "Â°c")),
            "power_w": self._pick_value(readings, ("cpu package power", "package power", "cpu power"), (), ("w",)),
            "cpu_power_w_hwinfo": self._pick_value(readings, ("cpu package power", "package power", "cpu power"), (), ("w",)),
            "gpu_power_w_hwinfo": self._pick_value(readings, ("gpu power",), ("memory", "fan"), ("w",)),
            "memory_power_w_or_proxy": memory_power_direct,
            "system_power_w": self._pick_value(readings, ("total system power",), (), ("w",)),
            "fan_rpm": self._average_values(readings, ("fan",), ("pump",), ("rpm",)),
            "pump_rpm": self._average_values(readings, ("pump",), (), ("rpm",)),
        }
        if metrics["memory_power_w_or_proxy"] is None:
            metrics["memory_power_w_or_proxy"] = metrics["memory_series"]
        metric_catalog = []
        for key, value in metrics.items():
            if value is None:
                continue
            if key == "memory_power_w_or_proxy":
                is_direct_memory_power = memory_power_direct is not None
                metric_catalog.append(
                    self._metric_catalog_entry(
                        key,
                        unit_override="W" if is_direct_memory_power else "",
                        name_override="Memory Power" if is_direct_memory_power else "Memory Power / Demand Proxy",
                    )
                )
            else:
                metric_catalog.append(self._metric_catalog_entry(key))
        return {"metrics": metrics, "metric_catalog": metric_catalog}

    def _curated_csv_metrics(self, headers: List[str], row: List[str]) -> Dict[str, object]:
        metrics: Dict[str, float] = {}
        metric_catalog: List[Dict[str, object]] = []
        for index, header in enumerate(headers):
            raw_header = str(header or "").strip()
            if not self._include_csv_header(raw_header):
                continue
            value = _safe_float(row[index] if index < len(row) else "")
            if value is None:
                continue
            metric_key = f"csv_c{index + 1:03d}_{_sanitize_metric_token(raw_header)}"
            metrics[metric_key] = value
            metric_catalog.append(
                {
                    "id": f"hwinfo__{metric_key}",
                    "display_name": f"HWiNFO CSV | {raw_header} [col {index + 1}]",
                    "unit": self._infer_unit(raw_header),
                    "source": "hwinfo",
                    "metric_key": metric_key,
                    "default_title": raw_header,
                    "default_xlabel": "Time (s)",
                    "default_ylabel": raw_header,
                    "raw_header": raw_header,
                    "column_index": index + 1,
                    "origin": "hwinfo_csv",
                }
            )
        return {"metrics": metrics, "metric_catalog": metric_catalog}

    def _metric_catalog_entry(
        self,
        metric_key: str,
        unit_override: Optional[str] = None,
        name_override: Optional[str] = None,
    ) -> Dict[str, object]:
        meta = self.METRIC_DEFS[metric_key]
        unit = meta["unit"] if unit_override is None else unit_override
        name = meta["name"] if name_override is None else name_override
        display_name = f"HWiNFO | {name} ({unit})" if unit else f"HWiNFO | {name}"
        ylabel = f"{name} ({unit})" if unit else name
        return {
            "id": f"hwinfo__{_sanitize_metric_token(metric_key)}",
            "display_name": display_name,
            "unit": unit,
            "source": "hwinfo",
            "metric_key": metric_key,
            "default_title": name,
            "default_xlabel": "Time (s)",
            "default_ylabel": ylabel,
            "raw_header": "",
            "column_index": 0,
            "origin": "hwinfo_normalized",
        }

    @staticmethod
    def _infer_unit(header: str) -> str:
        token = str(header).lower()
        bracket_match = re.search(r"\[([^\]]+)\]", token)
        if bracket_match:
            raw_unit = bracket_match.group(1).strip().replace("°", "")
            if raw_unit == "%":
                return "%"
            if raw_unit.lower() in {"w", "v", "rpm", "mhz", "ghz", "c"}:
                return raw_unit.upper()
        if "rpm" in token:
            return "RPM"
        if "volt" in token or " vid" in token or "vddq" in token:
            return "V"
        if "temp" in token or "temperature" in token or "Â°c" in token:
            return "C"
        if "power" in token:
            return "W"
        if "clock" in token or "frequency" in token:
            return "MHz"
        if "%" in token or "load" in token or "usage" in token:
            return "%"
        return ""

    @classmethod
    def _include_csv_header(cls, header: str) -> bool:
        token = str(header or "").strip().lower()
        if not token or token in {"date", "time"}:
            return False
        if any(key in token for key in cls.CURATED_CSV_INCLUDE_TOKENS):
            return True
        unit = cls._infer_unit(header)
        return unit in {"%", "W", "V", "C", "RPM", "MHz", "GHz"}


if __name__ == "__main__":
    collector = HWiNFOCollector({"enabled": True})
    print(json.dumps(collector.read_latest(), indent=2))


def binding_summary_json() -> str:
    return json.dumps(HWiNFOCollector.win32_binding_summary())
