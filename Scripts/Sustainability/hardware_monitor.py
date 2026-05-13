#!/usr/bin/env python3
"""Unified external collector runtime for MATLAB."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict

from hwinfo_collector import HWiNFOCollector
from icue_collector import ICUECollector


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _is_finite(value: object) -> bool:
    return isinstance(value, (int, float)) and value == value and value not in (float("inf"), float("-inf"))


class CollectorRuntimeFacade:
    """Python runtime entrypoint exposed to MATLAB."""

    METRIC_DOMAIN = {
        "cpu_proxy": "cpu",
        "gpu_series": "gpu",
        "memory_series": "memory",
        "cpu_voltage_v": "voltage",
        "gpu_voltage_v": "voltage",
        "memory_voltage_v": "voltage",
        "cpu_temp_c": "temperature",
        "power_w": "power",
        "cpu_power_w_hwinfo": "power",
        "gpu_power_w_hwinfo": "power",
        "memory_power_w_or_proxy": "power",
        "system_power_w": "power",
        "fan_rpm": "fan",
        "pump_rpm": "pump",
        "coolant_temp_c": "temperature",
        "device_battery_level": "battery",
    }

    PREFERRED_SOURCE = {
        "cpu": "hwinfo",
        "gpu": "hwinfo",
        "memory": "hwinfo",
        "temperature": "hwinfo",
        "power": "hwinfo",
        "voltage": "hwinfo",
        "psu": "hwinfo",
        "fan": "icue",
        "pump": "icue",
        "battery": "icue",
        "device": "icue",
    }

    OVERLAY_METRICS = ["cpu_proxy", "gpu_series", "memory_series", "system_power_w", "cpu_temp_c"]

    def __init__(self, output_dir: str = "../../sensor_logs", interval: float = 0.5) -> None:
        self.output_dir = str(output_dir)
        self.interval = float(interval)
        self.run_id = ""
        self.collectors: Dict[str, object] = {}
        self.last_sample: Dict = self._empty_sample()

    def start_session_json(self, run_id: str, settings_json: str = "{}") -> str:
        settings = self._load_json(settings_json)
        self.interval = self._resolve_interval(settings)
        self.run_id = str(run_id or "")
        self.collectors = self._build_collectors(settings)
        started = {
            "run_id": self.run_id,
            "timestamp_utc": _utc_now(),
            "collector_status": self._current_status(),
            "coverage_domains": self._coverage_domains(),
        }
        return json.dumps(started)

    def read_latest_json(self) -> str:
        raw_samples = {}
        for source, collector in self.collectors.items():
            raw_samples[source] = collector.read_latest()
        self.last_sample = self._normalize_samples(raw_samples)
        return json.dumps(self.last_sample)

    def source_status_json(self) -> str:
        return json.dumps(self._current_status())

    def describe_coverage_json(self) -> str:
        return json.dumps(
            {
                source: collector.describe_coverage()
                for source, collector in self.collectors.items()
            }
        )

    def stop_session_json(self) -> str:
        summary = {
            "run_id": self.run_id,
            "timestamp_utc": _utc_now(),
            "collector_status": self._current_status(),
            "coverage_domains": self._coverage_domains(),
            "raw_log_paths": self.last_sample.get("raw_log_paths", {}),
        }
        return json.dumps(summary)

    # Compatibility alias for legacy bridge code paths.
    start_logging = start_session_json
    stop_logging = stop_session_json

    def _build_collectors(self, settings: Dict) -> Dict[str, object]:
        sustainability = dict(settings.get("sustainability", settings))
        collectors_cfg = dict(sustainability.get("external_collectors", sustainability.get("collectors", {})))
        paths_cfg = dict(sustainability.get("collector_paths", {}))
        runtime_cfg = dict(sustainability.get("collector_runtime", {}))
        sample_interval = self._resolve_interval(settings)

        output_dir = runtime_cfg.get("session_output_dir", sustainability.get("output_dir", self.output_dir))

        configs = {
            "hwinfo": {
                "enabled": bool(collectors_cfg.get("hwinfo", False)),
                "exe_path": paths_cfg.get("hwinfo", ""),
                "transport_mode": runtime_cfg.get("hwinfo_transport_mode", "auto"),
                "launch_if_needed": runtime_cfg.get("hwinfo_launch_if_needed", True),
                "csv_target_path": runtime_cfg.get("hwinfo_csv_target_path", ""),
                "csv_target_dir": runtime_cfg.get("hwinfo_csv_target_dir", ""),
                "csv_path": runtime_cfg.get("hwinfo_csv_path", ""),
                "csv_dir": runtime_cfg.get("hwinfo_csv_dir", ""),
                "session_output_dir": output_dir,
                "shared_memory_blob_path": runtime_cfg.get("hwinfo_shared_memory_blob_path", ""),
                "sample_interval": sample_interval,
            },
            "icue": {
                "enabled": bool(collectors_cfg.get("icue", False)),
                "exe_path": paths_cfg.get("icue", ""),
                "csv_path": runtime_cfg.get("icue_csv_path", ""),
                "csv_dir": runtime_cfg.get("icue_csv_dir", ""),
                "session_output_dir": output_dir,
                "user_props_path": runtime_cfg.get("icue_user_props_path", ""),
                "config_path": runtime_cfg.get("icue_config_path", ""),
                "devices_path": runtime_cfg.get("icue_devices_path", ""),
                "sample_interval": sample_interval,
            },
        }

        return {
            "hwinfo": HWiNFOCollector(configs["hwinfo"]),
            "icue": ICUECollector(configs["icue"]),
        }

    def _normalize_samples(self, raw_samples: Dict[str, Dict]) -> Dict:
        source_series = {}
        source_status = {}
        coverage_domains = {}
        raw_log_paths = {}
        preferred_source = {}
        unified = {}
        metric_catalog = []
        probe_details = {}

        for source, sample in raw_samples.items():
            source_series[source] = dict(sample.get("metrics", {}))
            source_status[source] = sample.get("status", "disabled")
            coverage = sample.get("coverage", {})
            coverage_domains[source] = coverage.get("domains", [])
            raw_log_paths[source] = sample.get("raw_log_path", "")
            metric_catalog.extend(self._normalize_metric_catalog(source, sample.get("metric_catalog", [])))
            probe_details[source] = dict(sample.get("probe_details", {}))

        for metric_key, domain in self.METRIC_DOMAIN.items():
            preferred = self.PREFERRED_SOURCE.get(domain, "hwinfo")
            preferred_source[metric_key] = preferred
            ordered_sources = [preferred] + [src for src in ("hwinfo", "icue") if src != preferred]
            value = None
            for source in ordered_sources:
                metric_value = source_series.get(source, {}).get(metric_key)
                if _is_finite(metric_value):
                    value = float(metric_value)
                    break
            unified[metric_key] = value

        return {
            "timestamp_utc": _utc_now(),
            "metrics": unified,
            "collector_series": source_series,
            "collector_status": source_status,
            "coverage_domains": coverage_domains,
            "preferred_source": preferred_source,
            "raw_log_paths": raw_log_paths,
            "overlay_metrics": list(self.OVERLAY_METRICS),
            "collector_metric_catalog": metric_catalog,
            "hwinfo_transport": str(raw_samples.get("hwinfo", {}).get("transport", "none")),
            "hwinfo_status_reason": str(raw_samples.get("hwinfo", {}).get("status_reason", "")),
            "collector_probe_details": probe_details,
        }

    def _current_status(self) -> Dict[str, str]:
        return {
            source: collector.probe().get("status", "disabled")
            for source, collector in self.collectors.items()
        }

    def _coverage_domains(self) -> Dict[str, list]:
        return {
            source: collector.describe_coverage().get("domains", [])
            for source, collector in self.collectors.items()
        }

    @classmethod
    def _empty_sample(cls) -> Dict:
        return {
            "timestamp_utc": _utc_now(),
            "metrics": {},
            "collector_series": {"hwinfo": {}, "icue": {}},
            "collector_status": {"hwinfo": "disabled", "icue": "disabled"},
            "coverage_domains": {"hwinfo": [], "icue": []},
            "preferred_source": {},
            "raw_log_paths": {"hwinfo": "", "icue": ""},
            "overlay_metrics": list(cls.OVERLAY_METRICS),
            "collector_metric_catalog": [],
            "hwinfo_transport": "none",
            "hwinfo_status_reason": "",
            "collector_probe_details": {"hwinfo": {}, "icue": {}},
        }

    @staticmethod
    def _normalize_metric_catalog(source: str, catalog_items: object) -> list:
        if not isinstance(catalog_items, list):
            return []
        normalized = []
        for item in catalog_items:
            if not isinstance(item, dict):
                continue
            source_token = str(item.get("source", source) or source).strip().lower()
            metric_key = str(item.get("metric_key", "")).strip().lower()
            entry_id = str(item.get("id", "")).strip()
            display_name = str(item.get("display_name", "")).strip()
            if not source_token or not metric_key or not entry_id or not display_name:
                continue
            normalized.append(
                {
                    "id": entry_id,
                    "display_name": display_name,
                    "unit": str(item.get("unit", "")).strip(),
                    "source": source_token,
                    "metric_key": metric_key,
                    "default_title": str(item.get("default_title", display_name)).strip(),
                    "default_xlabel": str(item.get("default_xlabel", "Time (s)")).strip(),
                    "default_ylabel": str(item.get("default_ylabel", display_name)).strip(),
                    "raw_header": str(item.get("raw_header", "")).strip(),
                    "column_index": int(item.get("column_index", 0) or 0),
                    "origin": str(item.get("origin", "")).strip(),
                }
            )
        return normalized

    @staticmethod
    def _load_json(settings_json: str) -> Dict:
        text = str(settings_json or "").strip()
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def _resolve_interval(settings: Dict) -> float:
        sustainability = dict(settings.get("sustainability", settings))
        candidates = (
            sustainability.get("sample_interval", None),
            settings.get("sample_interval", None),
        )
        for candidate in candidates:
            try:
                value = float(candidate)
            except (TypeError, ValueError):
                continue
            if value > 0:
                return value
        return 0.5


# Backward-compatible alias for legacy bridge code.
SensorDataLogger = CollectorRuntimeFacade


if __name__ == "__main__":
    runtime = CollectorRuntimeFacade()
    runtime.start_session_json("debug_run", "{}")
    print(runtime.read_latest_json())
