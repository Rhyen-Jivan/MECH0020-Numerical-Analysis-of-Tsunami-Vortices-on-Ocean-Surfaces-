#!/usr/bin/env python3
"""HWiNFO Pro CLI controller for phase-scoped CSV-first telemetry."""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    import ctypes
except ImportError:  # pragma: no cover
    ctypes = None  # type: ignore

try:
    import winreg
except ImportError:  # pragma: no cover
    winreg = None  # type: ignore

from hwinfo_collector import HWiNFOCollector, _safe_float


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_ok(**kwargs: object) -> int:
    payload = {"ok": True, "timestamp_utc": _utc_now()}
    payload.update(kwargs)
    print(json.dumps(payload), end="")
    return 0


def _json_error(status: str, message: str, **kwargs: object) -> int:
    payload = {
        "ok": False,
        "status": status,
        "message": message,
        "timestamp_utc": _utc_now(),
    }
    payload.update(kwargs)
    print(json.dumps(payload), end="")
    return 1


def _load_config(path_value: str) -> Dict:
    path = Path(path_value)
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path_value: str, payload: Dict) -> None:
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _read_json(path_value: str) -> Dict:
    path = Path(path_value)
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _is_admin() -> bool:
    if ctypes is None:
        return False
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:  # pragma: no cover - defensive fallback
        return False


def _list_hwinfo_pids() -> List[int]:
    try:
        output = subprocess.check_output(
            ["tasklist", "/FI", "IMAGENAME eq HWiNFO64.exe", "/FO", "CSV", "/NH"],
            text=True,
            encoding="utf-8",
            errors="ignore",
        )
    except Exception:
        return []

    rows = list(csv.reader(output.splitlines()))
    pids: List[int] = []
    for row in rows:
        if len(row) < 2:
            continue
        if row[0].strip().strip('"').lower() != "hwinfo64.exe":
            continue
        try:
            pids.append(int(row[1].strip().strip('"')))
        except ValueError:
            continue
    return pids


def _resolve_launch_target(exe_path: Path) -> Dict[str, str]:
    launcher_path = exe_path.with_name("HWiNFO64Launcher.exe")
    if launcher_path.is_file():
        return {
            "strategy": "cmd_batch",
            "exe_path": str(exe_path),
            "launcher_path": str(launcher_path),
        }
    return {
        "strategy": "cmd_batch",
        "exe_path": str(exe_path),
        "launcher_path": "",
    }


def _best_effort_set_sensor_interval(ini_path: Path, poll_rate_ms: int) -> str:
    if not ini_path.is_file():
        return "ini_missing"
    try:
        lines = ini_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return "ini_read_failed"

    updated = False
    output_lines: List[str] = []
    in_settings = False
    saw_settings = False
    sensor_written = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            if in_settings and not sensor_written:
                output_lines.append(f"SensorInterval={poll_rate_ms}")
                sensor_written = True
                updated = True
            in_settings = stripped.lower() == "[settings]"
            saw_settings = saw_settings or in_settings
            output_lines.append(line)
            continue
        if in_settings and stripped.lower().startswith("sensorinterval="):
            output_lines.append(f"SensorInterval={poll_rate_ms}")
            sensor_written = True
            updated = True
            continue
        output_lines.append(line)

    if saw_settings and in_settings and not sensor_written:
        output_lines.append(f"SensorInterval={poll_rate_ms}")
        updated = True
    elif not saw_settings:
        output_lines.extend(["[Settings]", f"SensorInterval={poll_rate_ms}"])
        updated = True

    if not updated:
        return "ini_unchanged"

    try:
        ini_path.write_text("\n".join(output_lines) + "\n", encoding="utf-8")
        return "ini_updated"
    except Exception:
        return "ini_write_failed"


def _best_effort_set_write_direct(write_direct: bool) -> str:
    if winreg is None:
        return "registry_unavailable"
    try:
        key = winreg.CreateKey(winreg.HKEY_CURRENT_USER, r"Software\HWiNFO64\Sensors")
        with key:
            winreg.SetValueEx(key, "LoggingWriteDirect", 0, winreg.REG_DWORD, 1 if write_direct else 0)
        return "registry_updated"
    except Exception:
        return "registry_write_failed"


def _make_launch_batch(batch_path: Path, exe_path: Path, csv_path: Path, poll_rate_ms: int) -> None:
    install_dir = exe_path.parent
    script = "\r\n".join(
        [
            "@echo off",
            f'cd /d "{install_dir}"',
            f'"{exe_path.name}" -poll_rate={int(poll_rate_ms)} -l"{csv_path}"',
        ]
    ) + "\r\n"
    batch_path.write_text(script, encoding="utf-8")


def _wait_for_new_hwinfo_pid(before: Iterable[int], timeout_s: float) -> Optional[int]:
    before_set = set(before)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        after = _list_hwinfo_pids()
        new_pids = [pid for pid in after if pid not in before_set]
        if len(new_pids) == 1:
            return new_pids[0]
        time.sleep(0.25)
    return None


def _stop_pid(
    pid: int,
    timeout_s: float,
    image_name: str = "HWiNFO64.exe",
    allow_force: bool = False,
) -> Dict[str, object]:
    if pid <= 0:
        return {"stop_requested": False, "pid": pid, "stopped": True}

    kill_attempts: List[Dict[str, object]] = []
    def _issue_kill(force: bool) -> None:
        force_suffix = ["/F"] if force else []
        if image_name:
            image_result = subprocess.run(
                ["taskkill", "/IM", image_name, "/T", *force_suffix],
                capture_output=True,
                text=True,
            )
            kill_attempts.append({
                "mode": "image",
                "target": image_name,
                "force": force,
                "returncode": image_result.returncode,
            })

        if pid in _list_hwinfo_pids():
            pid_result = subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", *force_suffix],
                capture_output=True,
                text=True,
            )
            kill_attempts.append({
                "mode": "pid",
                "target": pid,
                "force": force,
                "returncode": pid_result.returncode,
            })

    def _wait_until_exit() -> bool:
        deadline = time.time() + timeout_s
        while time.time() < deadline and pid in _list_hwinfo_pids():
            time.sleep(0.25)
        return pid not in _list_hwinfo_pids()

    _issue_kill(force=False)
    stopped = _wait_until_exit()
    forced = False
    if (not stopped) and allow_force:
        _issue_kill(force=True)
        stopped = _wait_until_exit()
        forced = True

    return {
        "stop_requested": True,
        "pid": pid,
        "stopped": stopped,
        "timeout_s": timeout_s,
        "kill_attempts": kill_attempts,
        "forced": forced,
        "allow_force": allow_force,
    }


def _count_csv_rows(csv_path: Path) -> int:
    if not csv_path.is_file():
        return 0
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            return sum(1 for _ in handle)
    except UnicodeDecodeError:
        with csv_path.open("r", encoding="cp1252", errors="ignore", newline="") as handle:
            return sum(1 for _ in handle)


def _probe_csv(csv_path: Path) -> Dict[str, object]:
    exists = csv_path.is_file()
    row_count = _count_csv_rows(csv_path) if exists else 0
    return {
        "csv_exists": exists,
        "csv_row_count": row_count,
        "csv_has_data_rows": row_count > 1,
        "csv_path": str(csv_path),
    }


def _parse_timestamp(date_text: str, time_text: str, timezone_name: str) -> Optional[datetime]:
    date_token = str(date_text or "").strip()
    time_token = str(time_text or "").strip()
    if not date_token or not time_token:
        return None

    formats = [
        "%d.%m.%Y %H:%M:%S.%f",
        "%d.%m.%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S.%f",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            naive = datetime.strptime(f"{date_token} {time_token}", fmt)
            if ZoneInfo is not None:
                local_zone = ZoneInfo(timezone_name)
                aware = naive.replace(tzinfo=local_zone)
            else:  # pragma: no cover
                aware = naive.replace(tzinfo=timezone.utc)
            return aware.astimezone(timezone.utc)
        except Exception:
            continue
    return None


@dataclass
class BoundaryInterval:
    start_s: float
    end_s: float
    phase_id: str
    workflow_kind: str
    stage_id: str
    stage_label: str
    stage_type: str
    substage_id: str
    substage_label: str
    substage_type: str
    stage_method: str
    scenario_id: str
    mesh_level: Optional[float]
    mesh_nx: Optional[float]
    mesh_ny: Optional[float]
    child_run_index: Optional[float]


def _load_boundary_intervals(boundary_csv_path: Path) -> List[BoundaryInterval]:
    if not boundary_csv_path.is_file():
        return []
    with boundary_csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    intervals: List[BoundaryInterval] = []
    pending: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    order: List[Tuple[str, str, str]] = []
    for row in rows:
        event = str(row.get("boundary_event", "")).strip().lower()
        stage_id = str(row.get("stage_id", "") or "")
        substage_id = str(row.get("substage_id", "") or "")
        child_index = str(row.get("child_run_index", "") or "")
        key = (stage_id, substage_id, child_index)
        if event == "start":
            pending[key] = row
            order.append(key)
            continue
        if event != "end":
            continue
        start_row = pending.pop(key, None)
        if start_row is None:
            if order:
                fallback_key = order.pop(0)
                start_row = pending.pop(fallback_key, None)
        if start_row is None:
            continue
        try:
            start_s = float(start_row.get("session_time_s", "nan"))
            end_s = float(row.get("session_time_s", "nan"))
        except ValueError:
            continue
        intervals.append(
            BoundaryInterval(
                start_s=start_s,
                end_s=end_s,
                phase_id=str(start_row.get("phase_id", "") or ""),
                workflow_kind=str(start_row.get("workflow_kind", "") or ""),
                stage_id=stage_id,
                stage_label=str(start_row.get("stage_label", "") or ""),
                stage_type=str(start_row.get("stage_type", "") or ""),
                substage_id=substage_id,
                substage_label=str(start_row.get("substage_label", "") or ""),
                substage_type=str(start_row.get("substage_type", "") or ""),
                stage_method=str(start_row.get("stage_method", "") or ""),
                scenario_id=str(start_row.get("scenario_id", "") or ""),
                mesh_level=_safe_float(start_row.get("mesh_level", "")),
                mesh_nx=_safe_float(start_row.get("mesh_nx", "")),
                mesh_ny=_safe_float(start_row.get("mesh_ny", "")),
                child_run_index=_safe_float(start_row.get("child_run_index", "")),
            )
        )
    intervals.sort(key=lambda item: item.start_s)
    return intervals


def _interval_for_time(intervals: List[BoundaryInterval], session_time_s: float) -> Optional[BoundaryInterval]:
    epsilon = 1.0e-6
    for interval in intervals:
        if (interval.start_s - epsilon) <= session_time_s <= (interval.end_s + epsilon):
            return interval
    return None


def _open_csv_rows(csv_path: Path) -> Tuple[List[str], List[List[str]]]:
    encodings = ["utf-8-sig", "cp1252", "latin-1"]
    for encoding in encodings:
        try:
            with csv_path.open("r", encoding=encoding, newline="") as handle:
                rows = list(csv.reader(handle))
            if rows:
                return rows[0], rows[1:]
        except UnicodeDecodeError:
            continue
    return [], []


def _normalize_hwinfo_csv(raw_csv_path: Path, boundary_csv_path: Path, output_csv_path: Path, timezone_name: str) -> Dict[str, object]:
    headers, rows = _open_csv_rows(raw_csv_path)
    if not headers or not rows:
        raise RuntimeError("Raw HWiNFO CSV is empty or unreadable.")

    boundary_intervals = _load_boundary_intervals(boundary_csv_path)
    collector = HWiNFOCollector({"enabled": True})
    normalized_rows: List[Dict[str, object]] = []
    first_timestamp: Optional[datetime] = None
    assigned_rows = 0
    curated_metric_headers: List[str] = []

    for row in rows:
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        date_text = padded[0] if len(padded) >= 1 else ""
        time_text = padded[1] if len(padded) >= 2 else ""
        timestamp = _parse_timestamp(date_text, time_text, timezone_name)
        if timestamp is None:
            continue
        if first_timestamp is None:
            first_timestamp = timestamp
        session_time_s = max(0.0, (timestamp - first_timestamp).total_seconds())

        readings = [
            {
                "name": header,
                "sensor": "",
                "unit": collector._infer_unit(header),
                "value": _safe_float(padded[index] if index < len(padded) else ""),
            }
            for index, header in enumerate(headers)
        ]
        matched = collector._match_metrics(readings)
        curated = collector._curated_csv_metrics(headers, padded)
        metrics = dict(matched["metrics"])
        metrics.update(curated["metrics"])

        interval = _interval_for_time(boundary_intervals, session_time_s)
        if interval is not None:
            assigned_rows += 1
            stage_id = interval.stage_id
            stage_label = interval.stage_label
            stage_type = interval.stage_type
            stage_method = interval.stage_method
            substage_id = interval.substage_id
            substage_label = interval.substage_label
            substage_type = interval.substage_type
            scenario_id = interval.scenario_id
            phase_id = interval.phase_id
            workflow_kind = interval.workflow_kind
            mesh_level = interval.mesh_level
            mesh_nx = interval.mesh_nx
            mesh_ny = interval.mesh_ny
            child_run_index = interval.child_run_index
            stage_wall_time_meta = max(0.0, interval.end_s - interval.start_s)
        else:
            stage_id = "unassigned"
            stage_label = "Unassigned"
            stage_type = "overhead"
            stage_method = ""
            substage_id = ""
            substage_label = ""
            substage_type = ""
            scenario_id = ""
            phase_id = ""
            workflow_kind = ""
            mesh_level = None
            mesh_nx = None
            mesh_ny = None
            child_run_index = None
            stage_wall_time_meta = None

        record: Dict[str, object] = {
            "workflow_kind": workflow_kind,
            "phase_id": phase_id,
            "stage_id": stage_id,
            "stage_label": stage_label,
            "stage_type": stage_type,
            "stage_method": stage_method,
            "substage_id": substage_id,
            "substage_label": substage_label,
            "substage_type": substage_type,
            "scenario_id": scenario_id,
            "mesh_level": mesh_level,
            "mesh_index": mesh_level,
            "child_run_index": child_run_index,
            "t": session_time_s,
            "elapsed_wall_time": session_time_s,
            "wall_clock_time": timestamp.timestamp(),
            "iteration": None,
            "iter_rate": None,
            "iter_completion_pct": None,
            "mesh_nx": mesh_nx,
            "mesh_ny": mesh_ny,
            "cpu_proxy": metrics.get("cpu_proxy"),
            "gpu_series": metrics.get("gpu_series"),
            "memory_series": metrics.get("memory_series"),
            "cpu_temp_c": metrics.get("cpu_temp_c"),
            "power_w": metrics.get("power_w"),
            "cpu_voltage_v": metrics.get("cpu_voltage_v"),
            "gpu_voltage_v": metrics.get("gpu_voltage_v"),
            "memory_voltage_v": metrics.get("memory_voltage_v"),
            "cpu_power_w_hwinfo": metrics.get("cpu_power_w_hwinfo"),
            "gpu_power_w_hwinfo": metrics.get("gpu_power_w_hwinfo"),
            "memory_power_w_or_proxy": metrics.get("memory_power_w_or_proxy"),
            "system_power_w": metrics.get("system_power_w"),
            "environmental_energy_wh_cum": None,
            "environmental_co2_g_cum": None,
            "fan_rpm": metrics.get("fan_rpm"),
            "pump_rpm": metrics.get("pump_rpm"),
            "coolant_temp_c": metrics.get("coolant_temp_c"),
            "device_battery_level": metrics.get("device_battery_level"),
            "stage_wall_time_meta": stage_wall_time_meta,
            "hwinfo_status": "pro_cli_csv",
            "hwinfo_transport": "csv",
            "timestamp_utc": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source_date_local": date_text,
            "source_time_local": time_text,
        }

        curated_keys = sorted(key for key in metrics.keys() if key.startswith("csv_c"))
        curated_metric_headers = sorted(set(curated_metric_headers).union(curated_keys))
        for key in curated_keys:
            record[key] = metrics[key]

        normalized_rows.append(record)

    if not normalized_rows:
        raise RuntimeError("No valid timestamped HWiNFO rows were normalized.")

    fieldnames = [
        "workflow_kind",
        "phase_id",
        "stage_id",
        "stage_label",
        "stage_type",
        "stage_method",
        "substage_id",
        "substage_label",
        "substage_type",
        "scenario_id",
        "mesh_level",
        "mesh_index",
        "child_run_index",
        "t",
        "elapsed_wall_time",
        "wall_clock_time",
        "iteration",
        "iter_rate",
        "iter_completion_pct",
        "mesh_nx",
        "mesh_ny",
        "cpu_proxy",
        "gpu_series",
        "memory_series",
        "cpu_temp_c",
        "power_w",
        "cpu_voltage_v",
        "gpu_voltage_v",
        "memory_voltage_v",
        "cpu_power_w_hwinfo",
        "gpu_power_w_hwinfo",
        "memory_power_w_or_proxy",
        "system_power_w",
        "environmental_energy_wh_cum",
        "environmental_co2_g_cum",
        "fan_rpm",
        "pump_rpm",
        "coolant_temp_c",
        "device_battery_level",
        "stage_wall_time_meta",
        "hwinfo_status",
        "hwinfo_transport",
        "timestamp_utc",
        "source_date_local",
        "source_time_local",
    ] + curated_metric_headers

    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with output_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in normalized_rows:
            writer.writerow(record)

    return {
        "row_count": len(normalized_rows),
        "assigned_row_count": assigned_rows,
        "field_count": len(fieldnames),
        "output_csv_path": str(output_csv_path),
        "raw_csv_path": str(raw_csv_path),
        "boundary_csv_path": str(boundary_csv_path),
    }


def cmd_probe(config: Dict, run_id: str) -> int:
    exe_path = Path(str(config.get("exe_path", "") or ""))
    csv_path = Path(str(config.get("csv_path", "") or ""))
    probe = {
        "run_id": run_id,
        "exe_path": str(exe_path),
        "exe_exists": exe_path.is_file(),
        "csv_path": str(csv_path),
        "csv_parent_exists": csv_path.parent.is_dir(),
        "csv_probe": _probe_csv(csv_path),
        "is_admin": _is_admin(),
        "existing_hwinfo_pids": _list_hwinfo_pids(),
    }
    if exe_path.is_file():
        probe.update(_resolve_launch_target(exe_path))
    return _json_ok(status="probe_complete", probe=probe)


def cmd_start(config: Dict, run_id: str) -> int:
    exe_path = Path(str(config.get("exe_path", "") or ""))
    csv_path = Path(str(config.get("csv_path", "") or ""))
    session_json_path = Path(str(config.get("session_json_path", "") or ""))
    batch_path = Path(str(config.get("batch_script_path", "") or ""))
    poll_rate_ms = int(config.get("poll_rate_ms", 1000) or 1000)
    write_direct = bool(config.get("write_direct", True))
    launch_timeout_s = max(5.0, float(config.get("launch_timeout_s", 20.0) or 20.0))
    csv_timeout_s = max(5.0, float(config.get("csv_timeout_s", 45.0) or 45.0))
    force_stop_fallback = bool(config.get("force_stop_fallback", True))

    if not exe_path.is_file():
        return _json_error("hwinfo_executable_missing", "HWiNFO executable was not found.", exe_path=str(exe_path))
    if _list_hwinfo_pids():
        return _json_error(
            "hwinfo_process_already_running",
            "HWiNFO is already running. Pro CLI logging requires launching a new dedicated instance.",
            existing_hwinfo_pids=_list_hwinfo_pids(),
        )
    if not _is_admin():
        return _json_error(
            "admin_required",
            "HWiNFO Pro CLI logging requires an elevated launch context on this host.",
            exe_path=str(exe_path),
        )
    if not csv_path.parent.exists():
        csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not os.access(csv_path.parent, os.W_OK):
        return _json_error(
            "csv_target_not_writable",
            "The configured HWiNFO CSV directory is not writable.",
            csv_path=str(csv_path),
        )

    launch_target = _resolve_launch_target(exe_path)
    ini_state = _best_effort_set_sensor_interval(exe_path.with_suffix(".INI"), poll_rate_ms)
    registry_state = _best_effort_set_write_direct(write_direct)

    batch_path.parent.mkdir(parents=True, exist_ok=True)
    _make_launch_batch(batch_path, exe_path, csv_path, poll_rate_ms)

    before_pids = _list_hwinfo_pids()
    try:
        process = subprocess.Popen(
            ["cmd.exe", "/c", str(batch_path)],
            cwd=str(exe_path.parent),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception as exc:
        return _json_error("launch_failed", f"Could not launch the HWiNFO batch wrapper: {exc}")

    new_pid = _wait_for_new_hwinfo_pid(before_pids, timeout_s=launch_timeout_s)
    if new_pid is None:
        return _json_error(
            "launch_not_observed",
            "HWiNFO launch was not observed. This usually means UAC blocked the launch or the process exited immediately.",
            launcher_strategy=launch_target.get("strategy", ""),
            batch_path=str(batch_path),
            launch_timeout_s=launch_timeout_s,
        )

    deadline = time.time() + csv_timeout_s
    csv_probe = _probe_csv(csv_path)
    while time.time() < deadline and not bool(csv_probe["csv_has_data_rows"]):
        time.sleep(0.5)
        csv_probe = _probe_csv(csv_path)

    session_payload = {
        "run_id": run_id,
        "started_at_utc": _utc_now(),
        "pid": new_pid,
        "exe_path": str(exe_path),
        "csv_path": str(csv_path),
        "batch_path": str(batch_path),
        "poll_rate_ms": poll_rate_ms,
        "write_direct": write_direct,
        "ini_state": ini_state,
        "registry_state": registry_state,
        "launcher_strategy": launch_target.get("strategy", ""),
        "launcher_path": launch_target.get("launcher_path", ""),
        "wrapper_pid": process.pid,
        "launch_timeout_s": launch_timeout_s,
        "csv_timeout_s": csv_timeout_s,
        "force_stop_fallback": force_stop_fallback,
    }
    if session_json_path:
        _write_json(str(session_json_path), session_payload)

    if not bool(csv_probe["csv_has_data_rows"]):
        cleanup_state = _stop_pid(
            new_pid,
            timeout_s=10.0,
            image_name=exe_path.name,
            allow_force=force_stop_fallback,
        )
        return _json_error(
            "csv_not_materialized",
            "HWiNFO launched but did not materialize a non-empty CSV within the startup timeout.",
            session_json_path=str(session_json_path),
            csv_timeout_s=csv_timeout_s,
            cleanup_state=cleanup_state,
            **csv_probe,
        )

    return _json_ok(
        status="started",
        pid=new_pid,
        session_json_path=str(session_json_path),
        batch_path=str(batch_path),
        ini_state=ini_state,
        registry_state=registry_state,
        launcher_strategy=launch_target.get("strategy", ""),
        csv_path=str(csv_path),
        csv_row_count=csv_probe["csv_row_count"],
    )


def cmd_stop(config: Dict, run_id: str) -> int:
    session_json_path = Path(str(config.get("session_json_path", "") or ""))
    if not session_json_path.is_file():
        return _json_error("session_metadata_missing", "HWiNFO Pro session metadata JSON was not found.")
    session = _read_json(str(session_json_path))
    pid = int(session.get("pid", 0) or 0)
    csv_path = Path(str(session.get("csv_path", "") or ""))
    force_stop_fallback = bool(config.get("force_stop_fallback", session.get("force_stop_fallback", True)))
    if pid <= 0:
        return _json_error("session_pid_missing", "Session metadata did not include a valid HWiNFO PID.")

    image_name = Path(str(session.get("exe_path", "") or "")).name or "HWiNFO64.exe"
    stop_state = _stop_pid(pid, timeout_s=10.0, image_name=image_name, allow_force=force_stop_fallback)

    csv_probe = _probe_csv(csv_path)
    session["stopped_at_utc"] = _utc_now()
    session["stop_probe"] = csv_probe
    session["stop_state"] = stop_state
    session["force_stop_fallback"] = force_stop_fallback
    _write_json(str(session_json_path), session)

    if pid in _list_hwinfo_pids():
        return _json_error("graceful_stop_failed", "HWiNFO did not exit after the configured stop strategy.", stop_state=stop_state, **csv_probe)
    if not bool(csv_probe["csv_has_data_rows"]):
        return _json_error("csv_empty_after_stop", "HWiNFO exited but the CSV does not contain any data rows.", stop_state=stop_state, **csv_probe)

    return _json_ok(status="stopped", session_json_path=str(session_json_path), stop_state=stop_state, **csv_probe)


def cmd_normalize(config: Dict, run_id: str) -> int:
    raw_csv_path = Path(str(config.get("raw_csv_path", "") or ""))
    boundary_csv_path = Path(str(config.get("boundary_csv_path", "") or ""))
    output_csv_path = Path(str(config.get("output_csv_path", "") or ""))
    timezone_name = str(config.get("timezone_name", "UTC") or "UTC")

    if not raw_csv_path.is_file():
        return _json_error("raw_csv_missing", "Raw HWiNFO CSV was not found.", raw_csv_path=str(raw_csv_path))
    if not boundary_csv_path.is_file():
        return _json_error("boundary_csv_missing", "Telemetry boundary CSV was not found.", boundary_csv_path=str(boundary_csv_path))
    try:
        summary = _normalize_hwinfo_csv(raw_csv_path, boundary_csv_path, output_csv_path, timezone_name)
    except Exception as exc:
        return _json_error("normalize_failed", str(exc), raw_csv_path=str(raw_csv_path), boundary_csv_path=str(boundary_csv_path))
    return _json_ok(status="normalized", run_id=run_id, **summary)


def main() -> int:
    parser = argparse.ArgumentParser(description="HWiNFO Pro phase telemetry controller")
    parser.add_argument("command", choices=["start", "stop", "probe", "normalize"])
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", default="")
    args = parser.parse_args()

    config = _load_config(args.config)
    if args.command == "probe":
        return cmd_probe(config, args.run_id)
    if args.command == "start":
        return cmd_start(config, args.run_id)
    if args.command == "stop":
        return cmd_stop(config, args.run_id)
    if args.command == "normalize":
        return cmd_normalize(config, args.run_id)
    return _json_error("unsupported_command", f"Unsupported command: {args.command}")


if __name__ == "__main__":
    sys.exit(main())
