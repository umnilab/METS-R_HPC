"""Model and detect falsified vehicle trajectories.

This simulator-free demo follows the feature-extractor/classifier structure in
Ying et al., *Modeling and Detecting Falsified Vehicle Trajectories Under Data
Spoofing Attacks* (VehicleSec 2024 WIP): it learns from benign, ETA-delay, and
multi-sensor-fusion-drift trajectories, then measures generalisation to attack
families that were not used for training.  The default generator is deliberately
lightweight so the study can be inspected without Docker.  ``--input-csv`` can
instead consume METS-R/BSM traces with the documented schema.

This is a data/detector replication, not a reproduction of VASP or of the
paper's reported accuracy.  All generated attacks are synthetic and must only
be used in controlled experiments.

Paper: https://www.ndss-symposium.org/wp-content/uploads/vehiclesec2024-35-paper.pdf
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tutorials.security_demos.dashboard import serve_dashboard, write_dashboard  # noqa: E402


PAPER_URL = "https://www.ndss-symposium.org/wp-content/uploads/vehiclesec2024-35-paper.pdf"
KNOWN_ATTACKS = ("eta_delay", "msf_drift")
UNSEEN_ATTACKS = (
    "random_position",
    "random_position_offset",
    "high_acceleration",
    "low_speed",
    "communication_range_brake",
    "eebl_route_deviation",
)
FEATURE_NAMES = (
    "mean_abs_acceleration",
    "mean_abs_heading_rate",
    "mean_abs_gap_error",
    "mean_speed",
    "std_speed",
    "mean_acceleration",
    "std_acceleration",
    "mean_heading_rate",
    "std_heading_rate",
    "mean_gap_error",
    "std_gap_error",
    "max_speed",
    "max_acceleration",
)


@dataclass(frozen=True)
class Point:
    t: float
    x: float
    y: float
    speed: float
    acceleration: float
    heading: float
    lead_gap: float


@dataclass(frozen=True)
class Trajectory:
    trajectory_id: str
    points: tuple[Point, ...]
    attacked: bool
    attack_type: str
    split: str
    map_profile: str


@dataclass
class LogisticClassifier:
    means: list[float]
    scales: list[float]
    weights: list[float]
    bias: float
    threshold: float = 0.5

    def probability(self, features: Sequence[float]) -> float:
        z = self.bias
        for value, mean, scale, weight in zip(
            features, self.means, self.scales, self.weights
        ):
            z += weight * ((float(value) - mean) / scale)
        if z >= 0:
            return 1.0 / (1.0 + math.exp(-z))
        ez = math.exp(z)
        return ez / (1.0 + ez)

    def predict(self, features: Sequence[float]) -> bool:
        return self.probability(features) >= self.threshold


def _mean(values: Sequence[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def _sample_std(values: Sequence[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def _heading_delta(left: float, right: float) -> float:
    return (right - left + math.pi) % (2.0 * math.pi) - math.pi


def extract_features(points: Sequence[Point], time_headway: float = 1.5, min_gap: float = 4.0) -> list[float]:
    """Return the thirteen domain features described in the paper."""

    if len(points) < 3:
        raise ValueError("at least three trajectory points are required")
    accelerations = [point.acceleration for point in points]
    speeds = [point.speed for point in points]
    heading_rates: list[float] = []
    for left, right in zip(points, points[1:]):
        dt = max(1e-6, right.t - left.t)
        heading_rates.append(_heading_delta(left.heading, right.heading) / dt)
    gap_errors = [
        min_gap + time_headway * point.speed - point.lead_gap for point in points
    ]
    return [
        _mean([abs(value) for value in accelerations]),
        _mean([abs(value) for value in heading_rates]),
        _mean([abs(value) for value in gap_errors]),
        _mean(speeds),
        _sample_std(speeds),
        _mean(accelerations),
        _sample_std(accelerations),
        _mean(heading_rates),
        _sample_std(heading_rates),
        _mean(gap_errors),
        _sample_std(gap_errors),
        max(speeds),
        max(accelerations),
    ]


def _rebuild_points(
    base: Sequence[Point],
    *,
    speed: Sequence[float] | None = None,
    x_offset: Sequence[float] | None = None,
    y_offset: Sequence[float] | None = None,
    gap_offset: Sequence[float] | None = None,
) -> tuple[Point, ...]:
    count = len(base)
    speeds = [max(0.0, value) for value in (speed or [point.speed for point in base])]
    xo = list(x_offset or [0.0] * count)
    yo = list(y_offset or [0.0] * count)
    go = list(gap_offset or [0.0] * count)
    xs = [base[0].x + xo[0]]
    for index in range(1, count):
        dt = max(1e-6, base[index].t - base[index - 1].t)
        xs.append(xs[-1] + speeds[index] * dt + (xo[index] - xo[index - 1]))
    ys = [base[index].y + yo[index] for index in range(count)]
    accelerations = [base[0].acceleration]
    headings = [base[0].heading]
    for index in range(1, count):
        dt = max(1e-6, base[index].t - base[index - 1].t)
        accelerations.append((speeds[index] - speeds[index - 1]) / dt)
        headings.append(math.atan2(ys[index] - ys[index - 1], xs[index] - xs[index - 1]))
    return tuple(
        Point(
            t=base[index].t,
            x=xs[index],
            y=ys[index],
            speed=speeds[index],
            acceleration=accelerations[index],
            heading=headings[index],
            lead_gap=max(0.1, base[index].lead_gap + go[index] - (xs[index] - base[index].x)),
        )
        for index in range(count)
    )


def _benign_points(rng: random.Random, count: int, dt: float, map_profile: str) -> tuple[Point, ...]:
    phase = rng.uniform(0.0, math.tau)
    base_speed = rng.uniform(11.0, 18.0)
    curve_scale = {"straight": 0.08, "curved": 0.45, "dense": 0.18}[map_profile]
    speeds: list[float] = []
    ys: list[float] = []
    for index in range(count):
        wave = math.sin(index * 0.055 + phase)
        traffic = -1.8 * math.exp(-((index - count * 0.58) / max(5.0, count * 0.12)) ** 2) if map_profile == "dense" else 0.0
        speeds.append(max(2.0, base_speed + 0.7 * wave + traffic + rng.gauss(0.0, 0.025)))
        ys.append(curve_scale * math.sin(index * 0.025 + phase / 3.0) + rng.gauss(0.0, 0.006))
    xs = [rng.uniform(0.0, 30.0)]
    for index in range(1, count):
        xs.append(xs[-1] + speeds[index] * dt)
    accelerations = [0.0]
    headings = [0.0]
    for index in range(1, count):
        accelerations.append((speeds[index] - speeds[index - 1]) / dt)
        headings.append(math.atan2(ys[index] - ys[index - 1], xs[index] - xs[index - 1]))
    gaps = [
        max(4.5, 4.0 + 1.5 * speeds[index] + rng.gauss(0.0, 0.28))
        for index in range(count)
    ]
    return tuple(
        Point(index * dt, xs[index], ys[index], speeds[index], accelerations[index], headings[index], gaps[index])
        for index in range(count)
    )


def apply_attack(points: Sequence[Point], attack_type: str, rng: random.Random) -> tuple[Point, ...]:
    """Mutate reported values while leaving the synthetic true trace untouched."""

    count = len(points)
    start = max(3, int(count * 0.28))
    speed = [point.speed for point in points]
    xo = [0.0] * count
    yo = [0.0] * count
    go = [0.0] * count
    if attack_type == "eta_delay":
        for index in range(start, count):
            progress = (index - start) / max(1, count - start - 1)
            speed[index] *= 1.0 - 0.42 * math.sin(math.pi * progress) ** 2
    elif attack_type == "msf_drift":
        aggressive = int(count * 0.52)
        for index in range(start, count):
            if index < aggressive:
                yo[index] = rng.gauss(-0.02, 0.048)
            else:
                progress = (index - aggressive) / max(1, count - aggressive - 1)
                yo[index] = 0.05 + 3.2 * (math.exp(2.5 * progress) - 1.0) / (math.exp(2.5) - 1.0)
    elif attack_type == "random_position":
        for index in range(start, count):
            if rng.random() < 0.16:
                xo[index] = rng.uniform(-18.0, 18.0)
                yo[index] = rng.uniform(-2.5, 2.5)
    elif attack_type == "random_position_offset":
        drift = rng.choice((-1.0, 1.0)) * rng.uniform(8.0, 16.0)
        for index in range(start, count):
            progress = min(1.0, (index - start) / max(1, int(count * 0.16)))
            xo[index] = drift * progress
            go[index] = -drift * 0.55 * progress
    elif attack_type == "high_acceleration":
        burst = min(count - 1, start + max(3, count // 12))
        for index in range(start, burst):
            speed[index] += 4.5 * (index - start + 1) * (points[1].t - points[0].t)
        for index in range(burst, count):
            speed[index] += speed[burst - 1] - points[burst - 1].speed
    elif attack_type == "low_speed":
        for index in range(start, count):
            speed[index] *= 0.28
    elif attack_type == "communication_range_brake":
        for index in range(start, min(count, start + max(5, count // 10))):
            speed[index] = max(0.4, speed[index] - 0.62 * (index - start + 1))
        floor = speed[min(count - 1, start + max(4, count // 10) - 1)]
        for index in range(start + max(5, count // 10), count):
            speed[index] = max(floor, speed[index] * 0.5)
    elif attack_type == "eebl_route_deviation":
        for index in range(start, count):
            progress = (index - start) / max(1, count - start - 1)
            yo[index] = 2.2 * math.sin(progress * math.pi * 2.5)
            if 0.38 < progress < 0.56:
                speed[index] *= 0.38
    else:
        raise ValueError(f"unsupported attack type: {attack_type}")
    return _rebuild_points(points, speed=speed, x_offset=xo, y_offset=yo, gap_offset=go)


def generate_dataset(
    *,
    seed: int,
    points: int,
    train_benign: int,
    train_per_attack: int,
    test_benign: int,
    test_per_attack: int,
) -> list[Trajectory]:
    rng = random.Random(seed)
    trajectories: list[Trajectory] = []
    counter = 0

    def add(split: str, attack_type: str, count: int, profiles: Sequence[str]) -> None:
        nonlocal counter
        for _ in range(count):
            counter += 1
            profile = rng.choice(tuple(profiles))
            base = _benign_points(rng, points, 0.1, profile)
            attacked = attack_type != "benign"
            reported = apply_attack(base, attack_type, rng) if attacked else base
            trajectories.append(
                Trajectory(
                    trajectory_id=f"T{counter:05d}",
                    points=reported,
                    attacked=attacked,
                    attack_type=attack_type,
                    split=split,
                    map_profile=profile,
                )
            )

    add("train", "benign", train_benign, ("straight", "curved", "dense"))
    for attack in KNOWN_ATTACKS:
        add("train", attack, train_per_attack, ("straight", "curved", "dense"))
    add("test", "benign", test_benign, ("straight", "curved", "dense"))
    for attack in KNOWN_ATTACKS + UNSEEN_ATTACKS:
        # Reserve the dense profile as a mild map/domain shift for unseen attacks.
        profiles = ("curved", "dense") if attack in UNSEEN_ATTACKS else ("straight", "curved", "dense")
        add("test", attack, test_per_attack, profiles)
    rng.shuffle(trajectories)
    return trajectories


def _parse_bool_label(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "attack", "attacked", "malicious", "yes"}


def _finite_csv_float(value: str, *, trajectory_id: str, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"trajectory {trajectory_id!r} field {field!r} is not numeric: {value!r}"
        ) from error
    if not math.isfinite(number):
        raise ValueError(
            f"trajectory {trajectory_id!r} field {field!r} must be finite, got {value!r}"
        )
    return number


def load_trajectory_csv(path: Path, seed: int) -> list[Trajectory]:
    """Load long-form traces; see README for required and optional columns."""

    grouped: dict[str, list[Mapping[str, str]]] = defaultdict(list)
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"trajectory_id", "t", "x", "y", "speed", "acceleration", "heading", "lead_gap", "label"}
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"input CSV is missing columns: {', '.join(sorted(missing))}")
        for row in reader:
            grouped[str(row["trajectory_id"])].append(row)
    if len(grouped) < 4:
        raise ValueError("input CSV must contain at least four trajectories")
    rng = random.Random(seed)
    trajectories: list[Trajectory] = []
    for trajectory_id, rows in sorted(grouped.items()):
        numeric_fields = ("t", "x", "y", "speed", "acceleration", "heading", "lead_gap")
        parsed_rows = [
            (
                tuple(
                    _finite_csv_float(row[name], trajectory_id=trajectory_id, field=name)
                    for name in numeric_fields
                ),
                row,
            )
            for row in rows
        ]
        parsed_rows.sort(key=lambda item: item[0][0])
        first_row = parsed_rows[0][1]
        attacked = _parse_bool_label(first_row["label"])
        attack_type = (first_row.get("attack_type") or ("attack" if attacked else "benign")).strip()
        split = (first_row.get("split") or ("train" if rng.random() < 0.7 else "test")).strip().lower()
        profile = (first_row.get("map_profile") or "external").strip()
        points = tuple(Point(*values) for values, _row in parsed_rows)
        trajectories.append(Trajectory(trajectory_id, points, attacked, attack_type, split, profile))
    if not {trajectory.split for trajectory in trajectories} >= {"train", "test"}:
        raise ValueError("input CSV needs both train and test trajectories (explicitly or through the default split)")
    return trajectories


def train_classifier(rows: Sequence[Sequence[float]], labels: Sequence[bool], epochs: int = 900) -> LogisticClassifier:
    if len(rows) != len(labels) or not rows:
        raise ValueError("training rows and labels must be non-empty and aligned")
    if len(set(labels)) != 2:
        raise ValueError("training data must contain benign and attacked trajectories")
    width = len(rows[0])
    means = [_mean([float(row[index]) for row in rows]) for index in range(width)]
    scales = [max(1e-7, _sample_std([float(row[index]) for row in rows])) for index in range(width)]
    matrix = [[(float(value) - means[index]) / scales[index] for index, value in enumerate(row)] for row in rows]
    weights = [0.0] * width
    bias = 0.0
    positives = sum(bool(label) for label in labels)
    negatives = len(labels) - positives
    class_weights = {False: len(labels) / (2.0 * negatives), True: len(labels) / (2.0 * positives)}
    learning_rate = 0.075
    l2 = 0.015
    for epoch in range(epochs):
        gradient = [0.0] * width
        bias_gradient = 0.0
        for row, label in zip(matrix, labels):
            raw = bias + sum(weight * value for weight, value in zip(weights, row))
            probability = 1.0 / (1.0 + math.exp(-max(-35.0, min(35.0, raw))))
            error = (probability - float(label)) * class_weights[bool(label)]
            bias_gradient += error
            for index, value in enumerate(row):
                gradient[index] += error * value
        rate = learning_rate / (1.0 + epoch / 1400.0)
        for index in range(width):
            weights[index] -= rate * (gradient[index] / len(rows) + l2 * weights[index])
        bias -= rate * bias_gradient / len(rows)
    classifier = LogisticClassifier(means, scales, weights, bias)
    scores = [classifier.probability(row) for row in rows]
    classifier.threshold = choose_threshold(scores, labels)
    return classifier


def choose_threshold(scores: Sequence[float], labels: Sequence[bool]) -> float:
    candidates = sorted({0.05, 0.95, *(max(0.02, min(0.98, score)) for score in scores)})
    best = (float("-inf"), 0.5)
    for threshold in candidates:
        result = confusion(scores, labels, threshold)
        tpr = result["tp"] / max(1, result["tp"] + result["fn"])
        tnr = result["tn"] / max(1, result["tn"] + result["fp"])
        objective = (tpr + tnr) / 2.0 - 0.015 * abs(threshold - 0.5)
        if objective > best[0]:
            best = (objective, threshold)
    return best[1]


def confusion(scores: Sequence[float], labels: Sequence[bool], threshold: float) -> dict[str, int]:
    result = {"tp": 0, "tn": 0, "fp": 0, "fn": 0}
    for score, label in zip(scores, labels):
        predicted = score >= threshold
        key = "tp" if predicted and label else "fp" if predicted else "fn" if label else "tn"
        result[key] += 1
    return result


def classification_metrics(scores: Sequence[float], labels: Sequence[bool], threshold: float) -> dict[str, float]:
    c = confusion(scores, labels, threshold)
    total = sum(c.values())
    detection = c["tp"] / max(1, c["tp"] + c["fn"])
    false_positive = c["fp"] / max(1, c["fp"] + c["tn"])
    precision = c["tp"] / max(1, c["tp"] + c["fp"])
    return {
        **{key: float(value) for key, value in c.items()},
        "accuracy": (c["tp"] + c["tn"]) / max(1, total),
        "detection_rate": detection,
        "false_positive_rate": false_positive,
        "precision": precision,
        "f1": 2.0 * precision * detection / max(1e-12, precision + detection),
    }


def roc_points(scores: Sequence[float], labels: Sequence[bool]) -> list[list[float]]:
    points: list[list[float]] = []
    for step in range(101, -1, -1):
        threshold = step / 100.0
        metrics = classification_metrics(scores, labels, threshold)
        points.append([metrics["false_positive_rate"], metrics["detection_rate"]])
    points.sort(key=lambda pair: (pair[0], pair[1]))
    return points


def first_prefix_alert_time_from_trace_start(
    classifier: LogisticClassifier, trajectory: Trajectory
) -> float | None:
    """Return elapsed trace time to the first positive prefix classification.

    This exploratory value is intentionally measured from the trace's first
    sample, not from the modeled attack onset. ``None`` means the attacked
    trajectory never produced an alert and is therefore right-censored.
    """

    if not trajectory.attacked:
        return None
    minimum = min(len(trajectory.points), max(8, len(trajectory.points) // 8))
    for length in range(minimum, len(trajectory.points) + 1):
        features = extract_features(trajectory.points[:length])
        if classifier.predict(features):
            return trajectory.points[length - 1].t - trajectory.points[0].t
    return None


def _metric_group(
    classifier: LogisticClassifier,
    records: Sequence[dict[str, Any]],
    attack_types: set[str],
    include_benign: bool = True,
) -> dict[str, Any] | None:
    chosen = [
        record
        for record in records
        if record["attack_type"] in attack_types or (include_benign and not record["attacked"])
    ]
    eligible = [record for record in chosen if record["attacked"]]
    if not eligible:
        return None
    scores = [record["probability"] for record in chosen]
    labels = [record["attacked"] for record in chosen]
    metrics = classification_metrics(scores, labels, classifier.threshold)
    alert_times = [
        record["prefix_alert_time_from_trace_start_s"]
        for record in eligible
        if record["prefix_alert_time_from_trace_start_s"] is not None
    ]
    metrics["detected_only_mean_prefix_alert_time_from_trace_start_s"] = (
        _mean(alert_times) if alert_times else None
    )
    metrics["prefix_alert_detected_count"] = len(alert_times)
    metrics["prefix_alert_eligible_count"] = len(eligible)
    metrics["prefix_alert_censored_count"] = len(eligible) - len(alert_times)
    metrics["prefix_alert_caveat"] = (
        "Elapsed time starts at the trace's first sample, not attack onset. "
        "The mean includes detected attacked trajectories only; never-alerted "
        "attacks are right-censored and excluded."
    )
    metrics["trajectory_count"] = float(len(chosen))
    metrics["roc"] = roc_points(scores, labels)
    metrics["score_series"] = [
        [index / max(1, len(scores) - 1), value]
        for index, value in enumerate(sorted(scores))
    ]
    return metrics


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], fieldnames: Sequence[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_trajectory_csv(path: Path, trajectories: Sequence[Trajectory]) -> None:
    """Persist traces in the input schema with a canonical, round-trippable order."""

    fields = (
        "trajectory_id", "t", "x", "y", "speed", "acceleration", "heading",
        "lead_gap", "label", "attack_type", "split", "map_profile",
    )
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for trajectory in sorted(trajectories, key=lambda item: item.trajectory_id):
            context = {
                "trajectory_id": trajectory.trajectory_id,
                "label": int(trajectory.attacked),
                "attack_type": trajectory.attack_type,
                "split": trajectory.split,
                "map_profile": trajectory.map_profile,
            }
            for point in sorted(trajectory.points, key=lambda item: item.t):
                writer.writerow(
                    {
                        **context,
                        "t": point.t,
                        "x": point.x,
                        "y": point.y,
                        "speed": point.speed,
                        "acceleration": point.acceleration,
                        "heading": point.heading,
                        "lead_gap": point.lead_gap,
                    }
                )


def run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    trajectories = (
        load_trajectory_csv(Path(args.input_csv).expanduser().resolve(), args.seed)
        if args.input_csv
        else generate_dataset(
            seed=args.seed,
            points=args.points,
            train_benign=args.train_benign,
            train_per_attack=args.train_per_attack,
            test_benign=args.test_benign,
            test_per_attack=args.test_per_attack,
        )
    )
    feature_records: list[dict[str, Any]] = []
    for trajectory in trajectories:
        features = extract_features(trajectory.points)
        record: dict[str, Any] = {
            "trajectory_id": trajectory.trajectory_id,
            "split": trajectory.split,
            "attacked": trajectory.attacked,
            "attack_type": trajectory.attack_type,
            "map_profile": trajectory.map_profile,
        }
        record.update(dict(zip(FEATURE_NAMES, features)))
        feature_records.append(record)
    train_records = [record for record in feature_records if record["split"] == "train"]
    test_records = [record for record in feature_records if record["split"] == "test"]
    if not any(not record["attacked"] for record in test_records) or not any(
        record["attacked"] for record in test_records
    ):
        raise ValueError(
            "test data must contain at least one benign and one attacked trajectory"
        )
    train_rows = [[float(record[name]) for name in FEATURE_NAMES] for record in train_records]
    train_labels = [bool(record["attacked"]) for record in train_records]
    classifier = train_classifier(train_rows, train_labels, epochs=args.epochs)

    trajectory_by_id = {trajectory.trajectory_id: trajectory for trajectory in trajectories}
    predictions: list[dict[str, Any]] = []
    for record in test_records:
        features = [float(record[name]) for name in FEATURE_NAMES]
        probability = classifier.probability(features)
        trajectory = trajectory_by_id[str(record["trajectory_id"])]
        predictions.append(
            {
                **record,
                "probability": probability,
                "prediction": probability >= classifier.threshold,
                "correct": (probability >= classifier.threshold) == bool(record["attacked"]),
                "prefix_alert_time_from_trace_start_s": first_prefix_alert_time_from_trace_start(
                    classifier, trajectory
                ),
            }
        )

    training_attack_families = {
        str(record["attack_type"]) for record in train_records if record["attacked"]
    }
    test_attack_families = {
        str(record["attack_type"]) for record in predictions if record["attacked"]
    }
    known_attacks = training_attack_families & test_attack_families
    unseen_attacks = test_attack_families - training_attack_families
    known = _metric_group(classifier, predictions, known_attacks)
    unseen = _metric_group(classifier, predictions, unseen_attacks)
    combined = _metric_group(classifier, predictions, test_attack_families)
    if combined is None:
        raise ValueError("test data must contain at least one attacked trajectory")
    per_attack: dict[str, dict[str, float]] = {}
    benign_predictions = [record for record in predictions if not record["attacked"]]
    for attack in sorted(test_attack_families):
        subset = benign_predictions + [record for record in predictions if record["attack_type"] == attack]
        per_attack[attack] = classification_metrics(
            [record["probability"] for record in subset],
            [bool(record["attacked"]) for record in subset],
            classifier.threshold,
        )

    model = {
        "feature_names": FEATURE_NAMES,
        "means": classifier.means,
        "scales": classifier.scales,
        "weights": classifier.weights,
        "bias": classifier.bias,
        "threshold": classifier.threshold,
    }
    summary = {
        "paper": PAPER_URL,
        "paper_anchor": {
            "sample_counts": {"benign": 1000, "eta": 276, "msf": 464},
            "sampling_hz": 10,
            "svm_confusion": {"fp": 7, "fn": 1, "tp": 144, "tn": 196},
            "svm_accuracy_percent": 97.70,
            "warning": "Published anchors are context only, not acceptance targets for these synthetic results.",
        },
        "seed": args.seed,
        "source": str(Path(args.input_csv).resolve()) if args.input_csv else "synthetic",
        "training_attack_families": sorted(training_attack_families),
        "test_attack_families": sorted(test_attack_families),
        "known_attacks": sorted(known_attacks),
        "unseen_attacks": sorted(unseen_attacks),
        "evaluation_support": {
            "known": {
                "supported": known is not None,
                "attack_families": sorted(known_attacks),
                "attacked_trajectory_count": sum(
                    record["attacked"] and record["attack_type"] in known_attacks
                    for record in predictions
                ),
                "reason": (
                    None
                    if known is not None
                    else "No attacked family occurs in both training and test splits."
                ),
            },
            "unseen": {
                "supported": unseen is not None,
                "attack_families": sorted(unseen_attacks),
                "attacked_trajectory_count": sum(
                    record["attacked"] and record["attack_type"] in unseen_attacks
                    for record in predictions
                ),
                "reason": (
                    None
                    if unseen is not None
                    else "No test attack family is absent from the training split."
                ),
            },
            "combined": {
                "supported": True,
                "attack_families": sorted(test_attack_families),
                "attacked_trajectory_count": sum(record["attacked"] for record in predictions),
                "reason": None,
            },
        },
        "prefix_alert_metric": {
            "prediction_field": "prefix_alert_time_from_trace_start_s",
            "definition": "Elapsed time from the trajectory's first sample to its first positive prefix classification.",
            "aggregation": "Detected attacked trajectories only; never-alerted attacks are right-censored and excluded from the mean.",
            "not_attack_onset_latency": True,
        },
        "model": model,
        "metrics": {"known": known, "unseen": unseen, "combined": combined},
        "per_attack": per_attack,
    }
    (output_dir / "metrics.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    _write_trajectory_csv(output_dir / "trajectories.csv", trajectories)
    feature_fields = ("trajectory_id", "split", "attacked", "attack_type", "map_profile", *FEATURE_NAMES)
    _write_csv(output_dir / "trajectory_features.csv", feature_records, feature_fields)
    _write_csv(
        output_dir / "predictions.csv",
        predictions,
        (
            *feature_fields,
            "probability",
            "prediction",
            "correct",
            "prefix_alert_time_from_trace_start_s",
        ),
    )

    feature_importance = [
        [index + 1, abs(weight)] for index, weight in enumerate(classifier.weights)
    ]
    scenario_groups: list[tuple[str, str, str, dict[str, Any]]] = []
    if known is not None:
        scenario_groups.append(("known", "Known attacks", "#ffbd59", known))
    if unseen is not None:
        scenario_groups.append(("unseen", "Unseen attacks", "#ff6b6b", unseen))
    if known is not None and unseen is not None:
        scenario_groups.append(("combined", "Combined test", "#42d3a5", combined))

    scenarios = []
    for scenario_id, label, color, values in scenario_groups:
        events = [
            f"{name.replace('_', ' ')}: {per_attack[name]['detection_rate'] * 100:.1f}% detected"
            for name in sorted(per_attack)
            if (scenario_id == "combined")
            or (scenario_id == "known" and name in known_attacks)
            or (scenario_id == "unseen" and name in unseen_attacks)
        ]
        events.insert(
            0,
            "Prefix alerts: "
            f"{values['prefix_alert_detected_count']}/{values['prefix_alert_eligible_count']} "
            "eligible attacked trajectories; time-to-alert mean excludes never-alerted attacks.",
        )
        scenarios.append(
            {
                "id": scenario_id,
                "label": label,
                "color": color,
                "metrics": {
                    "detection_rate": values["detection_rate"] * 100.0,
                    "false_positive_rate": values["false_positive_rate"] * 100.0,
                    "accuracy": values["accuracy"] * 100.0,
                    "f1": values["f1"] * 100.0,
                    "detected_only_mean_prefix_alert_time_from_trace_start_s": values[
                        "detected_only_mean_prefix_alert_time_from_trace_start_s"
                    ],
                    "prefix_alert_detected_count": values["prefix_alert_detected_count"],
                    "prefix_alert_eligible_count": values["prefix_alert_eligible_count"],
                    "trajectory_count": values["trajectory_count"],
                },
                "series": {
                    "roc": [[x * 100.0, y * 100.0] for x, y in values["roc"]],
                    "scores": values["score_series"],
                    "feature_importance": feature_importance,
                },
                "events": events,
            }
        )
    dashboard = write_dashboard(
        {
            "title": "Falsified trajectory detector",
            "subtitle": (
                "Train on modeled spoofing families; test whether domain features transfer to test-only attacks."
                if unseen is not None
                else "Evaluate attack families represented in both training and test; this test split has no unseen family."
            ),
            "paper": {"title": "Modeling and Detecting Falsified Vehicle Trajectories", "venue": "VehicleSec 2024 WIP", "url": PAPER_URL},
            "scenarios": scenarios,
            "metric_cards": [
                {"key": "detection_rate", "label": "Detection rate", "unit": "%", "precision": 1},
                {"key": "false_positive_rate", "label": "False-positive rate", "unit": "%", "precision": 1},
                {"key": "accuracy", "label": "Accuracy", "unit": "%", "precision": 1},
                {"key": "f1", "label": "F1 score", "unit": "%", "precision": 1},
                {
                    "key": "detected_only_mean_prefix_alert_time_from_trace_start_s",
                    "label": "Detected-only prefix alert time",
                    "unit": "s",
                    "precision": 2,
                },
                {"key": "prefix_alert_detected_count", "label": "Attacks with prefix alert", "precision": 0},
                {"key": "prefix_alert_eligible_count", "label": "Attacks eligible for prefix alert", "precision": 0},
                {"key": "trajectory_count", "label": "Test trajectories", "precision": 0},
            ],
            "charts": [
                {"key": "roc", "title": "Receiver operating characteristic", "x_label": "false-positive rate (%)", "y_label": "detection rate (%)"},
                {"key": "scores", "title": "Classifier-score distribution", "x_label": "score quantile", "y_label": "attack probability"},
                {"key": "feature_importance", "title": "Absolute standardized model weights", "x_label": "feature number (f1…f13)", "y_label": "absolute weight", "wide": True,
                 "caption": "f1 mean |accel| · f2 mean |heading rate| · f3 mean |gap error| · f4 mean speed · f5 speed std · f6 mean accel · f7 accel std · f8 mean heading rate · f9 heading-rate std · f10 mean gap error · f11 gap-error std · f12 max speed · f13 max accel"},
            ],
            "parameters": {
                "seed": args.seed,
                "source": "external CSV" if args.input_csv else "synthetic at 10 Hz",
                "train trajectories": len(train_records),
                "test trajectories": len(predictions),
                "decision threshold": f"{classifier.threshold:.4f}",
                "known attack families in test": ", ".join(sorted(known_attacks)) or "none",
                "unseen attack families in test": ", ".join(sorted(unseen_attacks)) or "none",
            },
            "notes": [
                "The thirteen features follow the paper; the lightweight regularized logistic classifier is a transparent stand-in for its SVM/tree/random-forest evaluation.",
                "The paper's 1,000 benign / 276 ETA / 464 MSF sample counts and 97.70% SVM result are stored as contextual anchors in metrics.json, never as claims about this run.",
                (
                    "Unseen-attack accuracy is an outcome of this run and must not be presented as the paper's result or as production validation."
                    if unseen is not None
                    else "This test split has no attack family absent from training, so unseen metrics and the unseen dashboard scenario are omitted as unsupported."
                ),
                "Prefix time-to-alert is an added prefix-window experiment measured from the trace's first sample, not attack onset. Its mean includes detected attacks only; compare the detected and eligible counts because never-alerted attacks are excluded. The paper's reported detector starts after the complete trajectory is observed.",
                "Use --input-csv to replace the generator with labeled METS-R or BSM trajectories and repeat across maps, traffic densities, and attack magnitudes.",
            ],
        },
        output_dir,
    )
    print(f"Wrote {output_dir / 'metrics.json'}")
    print(f"Wrote {output_dir / 'trajectories.csv'}")
    print(f"Wrote {output_dir / 'trajectory_features.csv'}")
    print(f"Wrote {output_dir / 'predictions.csv'}")
    print(f"Wrote {dashboard}")
    if args.serve:
        serve_dashboard(dashboard, host=args.host, port=args.port, open_browser=args.open_browser)
    elif args.open_browser:
        import webbrowser

        webbrowser.open(dashboard.as_uri())
    return 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate/evaluate a falsified-trajectory detector and HTML dashboard.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input-csv", help="Optional long-form labeled trajectory CSV; otherwise generate data.")
    parser.add_argument("--seed", type=int, default=2403)
    parser.add_argument("--points", type=int, default=100, help="Points per generated 10 Hz trajectory.")
    parser.add_argument("--train-benign", type=int, default=180)
    parser.add_argument("--train-per-attack", type=int, default=70)
    parser.add_argument("--test-benign", type=int, default=90)
    parser.add_argument("--test-per-attack", type=int, default=35)
    parser.add_argument("--epochs", type=int, default=900)
    parser.add_argument(
        "--output-dir",
        default=str(_REPO_ROOT / "output" / "security_demos" / "trajectory_detector"),
    )
    parser.add_argument("--serve", action="store_true", help="Serve the finished dashboard until Ctrl-C.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0, help="Dashboard port; 0 chooses a free port.")
    parser.add_argument("--open-browser", action="store_true")
    args = parser.parse_args(argv)
    for name in ("points", "train_benign", "train_per_attack", "test_benign", "test_per_attack", "epochs"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.points < 12:
        parser.error("--points must be at least 12")
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
