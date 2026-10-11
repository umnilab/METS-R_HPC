"""Persist resolved experiment settings and per-step effect evidence."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .config import resolve_test_parameters


def write_test_report(result: Any, params: Mapping[str, Any], path: str | Path) -> None:
    """Write a profile report outside benchmark timing; normal traffic writes nothing."""
    safety = params.get("safety_scenario", "none")
    security = params.get("security_scenario", "none")
    if safety == security == "none":
        return
    records = result.records.get("test_events", ())
    timestep = float(params["timestep"])
    report = {
        "safety_scenario": safety,
        "security_scenario": security,
        "test_parameters": resolve_test_parameters(params.get("test_parameters")),
        "town": params.get("town"),
        "seed": params.get("seed"),
        "pcla_agent": params.get("pcla_agent"),
        "timestep_s": timestep,
        "termination_reason": str(result.terminationReason),
        "samples": [
            {"step": int(step), "simulation_time_s": step * timestep, **record}
            for step, record in records
        ],
    }
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
