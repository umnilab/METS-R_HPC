"""Live TRACR dashboard adapter for security_sim5g_v2x_examples.ipynb."""

from __future__ import annotations

import copy
import json
import math
import os
from collections import deque
from pathlib import Path
from typing import Any, MutableMapping, Sequence


_DEFINITION_MARKERS = (
    "RUN_CONFIG_PATH =",
    "def bridge_field",
    "def v2x_tick",
)

_PHASE_MARKERS = {
    "record": "metsr.generate_trip_between_roads(REPLAY_SOURCE_VID",
    "attack": 'if not replay_buffer:',
    "normal": "normal_rows = []",
    "summary": "all_rows = record_rows + attack_rows + normal_rows",
}


def _read_code_cells(notebook_path: Path) -> list[str]:
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    return [
        "".join(cell.get("source", ()))
        for cell in notebook.get("cells", ())
        if cell.get("cell_type") == "code"
    ]


def _find_code_cell(code_cells: list[str], marker: str) -> str:
    matches = [source for source in code_cells if marker in source]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected one code cell containing {marker!r}; found {len(matches)}. "
            "The security notebook layout may have changed."
        )
    return matches[0]


def load_security_scenario_definitions(
    notebook_path: str | Path,
    namespace: MutableMapping[str, Any],
) -> dict[str, Any]:
    """Load the security notebook's constants and pure helper definitions.

    The setup cell containing ``clear_all()`` and the cells which launch or stop
    external services are deliberately not executed.
    """

    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    from IPython.display import display

    from clients.KafkaDataProcessor import (
        bsm_core_heading_degrees,
        bsm_core_speed_mps,
        normalize_sensor_record,
    )
    from clients.METSRClient import METSRClient
    from clients.VeinsClient import VeinsClient, build_mobility_records
    from tutorials.v2x_veins_example import communication_records_from_result
    from utils.util import prepare_sim_dirs, read_run_config, run_simulation_in_docker

    notebook_path = Path(notebook_path).resolve()
    code_cells = _read_code_cells(notebook_path)
    defaults = {
        "Path": Path,
        "copy": copy,
        "json": json,
        "math": math,
        "os": os,
        "deque": deque,
        "pd": pd,
        "np": np,
        "plt": plt,
        "display": display,
        "METSRClient": METSRClient,
        "VeinsClient": VeinsClient,
        "build_mobility_records": build_mobility_records,
        "bsm_core_heading_degrees": bsm_core_heading_degrees,
        "bsm_core_speed_mps": bsm_core_speed_mps,
        "normalize_sensor_record": normalize_sensor_record,
        "communication_records_from_result": communication_records_from_result,
        "read_run_config": read_run_config,
        "prepare_sim_dirs": prepare_sim_dirs,
        "run_simulation_in_docker": run_simulation_in_docker,
    }
    for name, value in defaults.items():
        namespace.setdefault(name, value)

    for marker in _DEFINITION_MARKERS:
        source = _find_code_cell(code_cells, marker)
        exec(compile(source, f"{notebook_path}::{marker}", "exec"), namespace)

    return {
        "notebook_path": notebook_path,
        "code_cells": code_cells,
        "run_config": namespace["RUN_CONFIG_PATH"],
    }


class SecuritySim5GDashboardScenario:
    """Execute the security notebook phases with live TRACR visualization."""

    def __init__(
        self,
        *,
        notebook_path: str | Path,
        namespace: MutableMapping[str, Any],
        runtime: Any,
        dashboard: Any,
        cosim_support: Any,
        render_every: int = 2,
        dashboard_every: int = 6,
        sensor_every: int = 1,
        render_wait_timeout: float = 0,
        background_vehicle_ids: Sequence[int] = (),
    ):
        self.notebook_path = Path(notebook_path).resolve()
        self.namespace = namespace
        self.runtime = runtime
        self.dashboard = dashboard
        self.cosim_support = cosim_support
        self.render_every = max(1, int(render_every))
        self.dashboard_every = max(1, int(dashboard_every))
        self.sensor_every = max(1, int(sensor_every))
        self.render_wait_timeout = render_wait_timeout
        self.background_vehicle_ids = list(dict.fromkeys(background_vehicle_ids or ()))
        self.code_cells = _read_code_cells(self.notebook_path)
        self.deps = cosim_support._deps()
        self.visual_tick = 0
        self.last_result = None

        namespace.update(
            {
                "config": runtime.config,
                "metsr": runtime.metsr,
                "veins": runtime.veins_client,
                "sim_dirs": runtime.sim_dirs,
            }
        )
        reserved_ids = {
            str(namespace["REPLAY_SOURCE_VID"]),
            str(namespace["EGO_VID"]),
            str(namespace["REPLAY_SOURCE_RADIO_ID"]),
        }
        conflicting_ids = [
            vehicle_id
            for vehicle_id in self.background_vehicle_ids
            if str(vehicle_id) in reserved_ids
        ]
        if conflicting_ids:
            raise ValueError(
                f"Background vehicle IDs conflict with replay/ego IDs: {conflicting_ids}"
            )
        runtime.background_vehicle_ids = list(self.background_vehicle_ids)

        original_bsm_builder = namespace.get("make_basic_safety_message")
        original_bsm_builder = getattr(
            original_bsm_builder,
            "_security_dashboard_original",
            original_bsm_builder,
        )
        if not callable(original_bsm_builder):
            raise RuntimeError(
                "The security notebook did not define make_basic_safety_message()."
            )

        def georeferenced_bsm_builder(sender, tick, sequence):
            return original_bsm_builder(
                self._vehicle_with_geolocation(sender), tick, sequence
            )

        georeferenced_bsm_builder._security_dashboard_original = original_bsm_builder
        namespace["make_basic_safety_message"] = georeferenced_bsm_builder

        original_spawn_ego = namespace.get("spawn_ego_vehicle")
        original_spawn_ego = getattr(
            original_spawn_ego,
            "_security_dashboard_original",
            original_spawn_ego,
        )
        if not callable(original_spawn_ego):
            raise RuntimeError("The security notebook did not define spawn_ego_vehicle().")

        def spawn_ego_with_background(metsr):
            state = original_spawn_ego(metsr)
            self._generate_background_traffic()
            return state

        spawn_ego_with_background._security_dashboard_original = original_spawn_ego
        namespace["spawn_ego_vehicle"] = spawn_ego_with_background

        original_v2x_tick = namespace.get("v2x_tick")
        original_v2x_tick = getattr(
            original_v2x_tick, "_security_dashboard_original", original_v2x_tick
        )
        if not callable(original_v2x_tick):
            raise RuntimeError("Load the security scenario definitions before creating the adapter.")
        self.original_v2x_tick = original_v2x_tick

        def dashboard_v2x_tick(veins, tick, vehicles, messages, phase):
            result, rows = self.original_v2x_tick(
                veins, tick, vehicles, messages, phase
            )
            self._refresh_dashboard(phase, rows)
            return result, rows

        dashboard_v2x_tick._security_dashboard_original = original_v2x_tick
        namespace["v2x_tick"] = dashboard_v2x_tick

    def _vehicle_with_geolocation(self, vehicle) -> dict[str, Any]:
        record = dict(vehicle or {})
        if record.get("latitude") is not None and record.get("longitude") is not None:
            return record
        if record.get("x") is None or record.get("y") is None:
            return record

        world = getattr(self.runtime, "world", None)
        if world is None:
            return record
        try:
            location = self.deps["metsr_to_carla_location"](
                world,
                record["x"],
                record["y"],
                z=record.get("z", 0.0),
                snap=False,
                z_offset=0.0,
            )
            geolocation = world.get_map().transform_to_geolocation(location)
            latitude = float(geolocation.latitude)
            longitude = float(geolocation.longitude)
            if math.isfinite(latitude) and math.isfinite(longitude):
                record["latitude"] = latitude
                record["longitude"] = longitude
        except (AttributeError, KeyError, TypeError, ValueError, RuntimeError):
            pass
        return record

    def _generate_background_traffic(self) -> None:
        if not self.background_vehicle_ids:
            return
        response = self.runtime.metsr.generate_trip(
            self.background_vehicle_ids,
            -1,
            -1,
        )
        if isinstance(response, dict) and response.get("status") not in {
            None,
            "ok",
            "partial",
        }:
            raise RuntimeError(
                f"METS-R rejected background traffic generation: {response}"
            )

    def _phase_vehicle_id(self, phase: str):
        if phase == "record":
            return self.namespace["REPLAY_SOURCE_VID"]
        return self.namespace["EGO_VID"]

    def _configure_phase(self, phase: str) -> None:
        vehicle_id = self._phase_vehicle_id(phase)
        self.runtime.focus_vehicle_id = vehicle_id
        self.runtime.generated_vehicle_ids = [
            vehicle_id,
            *self.background_vehicle_ids,
        ]
        self.runtime.v2x_vehicle_ids = [vehicle_id]
        self.runtime.attack_vehicle_ids = (
            [self.namespace["REPLAY_SOURCE_RADIO_ID"]] if phase == "attack" else []
        )
        self.runtime._tracr_last_bsm_records = []

    @staticmethod
    def _dashboard_records(rows) -> list[dict[str, Any]]:
        records = []
        for row in rows or ():
            record = dict(row)
            sender_id = record.get("origin_vehicle_id")
            receiver_id = record.get("target_vehicle_id")
            record.setdefault("_topic", "v2x_rx_bsm")
            record.setdefault("sender_id", sender_id)
            record.setdefault("vehicle_id", sender_id)
            record.setdefault("receiver_id", receiver_id)
            records.append(record)
        return records

    def _refresh_dashboard(self, phase: str, rows) -> None:
        runtime = self.runtime
        records = self._dashboard_records(rows)
        runtime._tracr_last_bsm_records = list(records)

        projection_info = self.cosim_support._sync_tracr_road_context_vehicles(
            runtime, self.deps
        )
        sensors_now = self.visual_tick % self.sensor_every == 0
        if sensors_now and runtime.sensor_panel is not None:
            preferred = [projection_info.get("focus_vehicle")]
            preferred.extend(getattr(runtime, "v2x_vehicle_ids", ()) or ())
            runtime.sensor_panel.ensure_sensors(
                runtime.carla_state,
                preferred_vehicle_ids=[item for item in preferred if item is not None],
            )

        if runtime.world is not None:
            runtime.world.tick()
        self.cosim_support._keep_carla_projection_passive(
            runtime.carla_state, self.deps["carla"]
        )

        render_info = {"skipped": True}
        render_error = None
        if self.visual_tick % self.render_every == 0:
            try:
                render_info = runtime.metsr.render(
                    client_wait_timeout=self.render_wait_timeout
                )
                if isinstance(render_info, dict):
                    render_info["tracr_viz_selected_vehicle_id"] = (
                        projection_info.get("focus_vehicle")
                    )
            except Exception as exc:
                render_error = str(exc).splitlines()[0]

        step_result = {
            "state": runtime.carla_state,
            "vehicles": [],
            "fleet_controlled": True,
            "tracr_projection": projection_info,
            "security_phase": phase,
        }
        if self.visual_tick % self.dashboard_every == 0:
            self.dashboard.update(
                runtime,
                step_result,
                records,
                render_info=render_info,
                render_error=render_error,
            )

        self.last_result = {
            "phase": phase,
            "step_result": step_result,
            "bsm_records": records,
            "render_info": render_info,
            "render_error": render_error,
        }
        self.visual_tick += 1

    def _run_cell(self, marker: str) -> None:
        source = _find_code_cell(self.code_cells, marker)
        exec(compile(source, f"{self.notebook_path}::{marker}", "exec"), self.namespace)

    def run_record_phase(self) -> None:
        self._configure_phase("record")
        self._run_cell(_PHASE_MARKERS["record"])

    def run_attack_phase(self) -> None:
        self._configure_phase("attack")
        self._run_cell(_PHASE_MARKERS["attack"])

    def run_normal_phase(self) -> None:
        self._configure_phase("normal")
        self._run_cell(_PHASE_MARKERS["normal"])

    def summarize(self) -> dict[str, Any]:
        self._run_cell(_PHASE_MARKERS["summary"])
        return {
            "summary": self.namespace.get("summary"),
            "bsm_replay_result": self.namespace.get("bsm_replay_result"),
            "last_dashboard_result": self.last_result,
        }

    def run_all(self) -> dict[str, Any]:
        self.run_record_phase()
        self.run_attack_phase()
        self.run_normal_phase()
        return self.summarize()
