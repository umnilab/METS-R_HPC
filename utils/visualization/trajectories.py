"""Locate recorded simulation output and inspect trajectory manifests.

These helpers resolve run folders and configured output roots, prefer binary
trajectory formats when requested, and summarize manifests without starting
servers or importing the METS-R client.
"""

import json
import os
import time

from utils.configuration import _read_property_values


def _simulation_folder_from_config(config, sim_index=0, output_root="output"):
    sim_dirs = getattr(config, "sim_dirs", None)
    if sim_dirs:
        try:
            return sim_dirs[sim_index]
        except IndexError as exc:
            raise IndexError(
                f"sim_index {sim_index} is outside config.sim_dirs with {len(sim_dirs)} entries"
            ) from exc

    sim_folder = getattr(config, "sim_folder", None)
    if sim_folder:
        if isinstance(sim_folder, (list, tuple)):
            try:
                return sim_folder[sim_index]
            except IndexError as exc:
                raise IndexError(
                    f"sim_index {sim_index} is outside config.sim_folder with {len(sim_folder)} entries"
                ) from exc
        return sim_folder

    sim_dir = getattr(config, "sim_dir", None)
    if sim_dir:
        sim_dir_candidates = sim_dir if isinstance(sim_dir, (list, tuple)) else [sim_dir]
        try:
            sim_dir_candidate = sim_dir_candidates[sim_index]
        except IndexError as exc:
            raise IndexError(
                f"sim_index {sim_index} is outside config.sim_dir with {len(sim_dir_candidates)} entries"
            ) from exc
        if sim_dir_candidate and os.path.isdir(sim_dir_candidate):
            for root in _configured_trajectory_roots(sim_dir_candidate):
                if _latest_trajectory_directory(root, prefer_binary=False) is not None:
                    return sim_dir_candidate

    run_name = getattr(config, "name", None)
    if not run_name:
        raise ValueError(
            "Cannot infer simulation output folder: config needs sim_dirs, sim_folder, or a name."
        )

    seeds = getattr(config, "random_seeds", None) or []
    seed = seeds[sim_index] if sim_index < len(seeds) else None
    output_root = os.path.abspath(output_root)
    try:
        names = os.listdir(output_root)
    except OSError as exc:
        raise FileNotFoundError(f"Simulation output root does not exist: {output_root}") from exc

    prefix = f"{run_name}_"
    seed_suffix = f"_seed_{seed}" if seed is not None else None
    candidates = []
    for name in names:
        if not name.startswith(prefix):
            continue
        if seed_suffix is not None and not name.endswith(seed_suffix):
            continue
        candidate = os.path.join(output_root, name)
        if os.path.isdir(candidate):
            candidates.append(candidate)

    if not candidates:
        seed_text = f" and seed {seed}" if seed is not None else ""
        raise FileNotFoundError(
            f"No finished simulation output folder found for config name {run_name!r}{seed_text} under {output_root}"
        )

    return max(candidates, key=os.path.getmtime)


def latest_trajectory_output_dir_from_config(
        config,
        sim_index=0,
        trajectory_output_dir=None,
        prefer_binary=True,
        wait_seconds=0,
        output_root="output"):
    sim_folder = _simulation_folder_from_config(
        config,
        sim_index=sim_index,
        output_root=output_root,
    )
    if trajectory_output_dir is not None:
        roots = [_resolve_trajectory_root(sim_folder, trajectory_output_dir)]
    else:
        roots = _configured_trajectory_roots(sim_folder)

    deadline = time.time() + max(0, float(wait_seconds or 0))
    while True:
        for root in roots:
            latest_directory = _latest_trajectory_directory(root, prefer_binary=prefer_binary)
            if latest_directory is not None:
                return latest_directory

        if time.time() >= deadline:
            break
        time.sleep(0.5)

    roots_text = ", ".join(str(root) for root in roots if root)
    raise FileNotFoundError("No trajectory output directory found under " + roots_text)


def _read_trajectory_manifest(directory):
    manifest_path = os.path.join(directory, "manifest.json")
    try:
        with open(manifest_path, "r") as manifest_file:
            return json.load(manifest_file)
    except (OSError, json.JSONDecodeError):
        return None


def _resolve_trajectory_root(sim_folder, configured_path):
    if not configured_path:
        return None
    configured_path = os.path.normpath(configured_path)
    if os.path.isabs(configured_path):
        return configured_path
    return os.path.join(sim_folder, configured_path)


def _configured_trajectory_roots(sim_folder):
    properties = _read_property_values(os.path.join(sim_folder, "data", "Data.properties"))
    roots = []
    for key in ("TRAJECTORY_BINARY_DEFAULT_PATH", "JSON_DEFAULT_PATH"):
        root = _resolve_trajectory_root(sim_folder, properties.get(key))
        if root and root not in roots:
            roots.append(root)

    default_root = os.path.join(sim_folder, "trajectory_output")
    if default_root not in roots:
        roots.append(default_root)
    return roots


def _trajectory_format_score(directory):
    try:
        names = os.listdir(directory)
    except OSError:
        return 0

    if "manifest.json" in names:
        return 3
    lowered = [name.lower() for name in names]
    if any(name.endswith(".bin") for name in lowered):
        return 2
    if any(name.endswith(".json") for name in lowered):
        return 1
    return 0


def _trajectory_format_name(directory):
    manifest = _read_trajectory_manifest(directory)
    if manifest is not None:
        output_format = manifest.get("format", "binary")
        version = manifest.get("version")
        sparse_frame_groups = manifest.get("sparseFrameGroups") or []
        sparse_suffix = " sparse" if sparse_frame_groups else ""
        if version is not None:
            return f"{output_format} v{version}{sparse_suffix}"
        return f"{output_format}{sparse_suffix}"

    score = _trajectory_format_score(directory)
    if score >= 2:
        return "binary"
    if score == 1:
        return "JSON"
    return "trajectory"


def _trajectory_manifest_summary(directory, manifest):
    chunks = manifest.get("chunks", [])
    active_chunk = manifest.get("activeChunk", {})
    road_dictionary = manifest.get("roadIdDictionary", [])
    zone_dictionary = manifest.get("zoneDictionary", [])
    charging_station_dictionary = manifest.get("chargingStationDictionary", [])
    schemas = manifest.get("schemas", {})
    frame_groups = manifest.get("frameGroups", [])
    sparse_frame_groups = manifest.get("sparseFrameGroups") or []
    sparse_frame_group_mode = manifest.get("sparseFrameGroupMode")

    return {
        "directory": directory,
        "manifest_path": os.path.join(directory, "manifest.json"),
        "format": manifest.get("format"),
        "version": manifest.get("version"),
        "byte_order": manifest.get("byteOrder"),
        "coord_scale": manifest.get("coordScale"),
        "initial_x": manifest.get("initialX"),
        "initial_y": manifest.get("initialY"),
        "tick_interval": manifest.get("tickInterval"),
        "link_snapshot_interval": manifest.get("linkSnapshotInterval"),
        "chunk_tick_limit": manifest.get("chunkTickLimit"),
        "chunk_count": len(chunks),
        "active_chunk": active_chunk,
        "road_count": len(road_dictionary),
        "zone_count": len(zone_dictionary),
        "charging_station_count": len(charging_station_dictionary),
        "frame_groups": frame_groups,
        "sparse_frame_groups": sparse_frame_groups,
        "sparse_frame_group_mode": sparse_frame_group_mode,
        "has_sparse_frame_groups": bool(sparse_frame_groups),
        "has_sparse_zone_frames": "zone" in sparse_frame_groups,
        "has_sparse_charging_station_frames": "chargingStation" in sparse_frame_groups,
        "schema_names": sorted(schemas.keys()),
        "has_zone_attributes": bool(zone_dictionary) or "zone" in schemas,
        "has_charging_station_attributes": (
            bool(charging_station_dictionary) or "chargingStation" in schemas
        ),
        "has_split_energy_fields": (
            "frameHeader" in schemas
            and any("energyPrivateEV" in field for field in schemas["frameHeader"])
        ),
    }


def _latest_trajectory_directory(root, prefer_binary=True):
    if root is None or not os.path.isdir(root):
        return None

    candidates = []
    root_score = _trajectory_format_score(root)
    if root_score > 0:
        candidates.append((root_score, os.path.getmtime(root), root))

    for name in os.listdir(root):
        candidate = os.path.join(root, name)
        if not os.path.isdir(candidate):
            continue
        score = _trajectory_format_score(candidate)
        if score > 0:
            candidates.append((score, os.path.getmtime(candidate), candidate))

    if not candidates:
        subdirs = [
            os.path.join(root, name)
            for name in os.listdir(root)
            if os.path.isdir(os.path.join(root, name))
        ]
        if not subdirs:
            return None
        return max(subdirs, key=os.path.getmtime)

    if prefer_binary:
        binary_candidates = [candidate for candidate in candidates if candidate[0] >= 2]
        if binary_candidates:
            return max(binary_candidates, key=lambda item: item[1])[2]

    return max(candidates, key=lambda item: item[1])[2]
