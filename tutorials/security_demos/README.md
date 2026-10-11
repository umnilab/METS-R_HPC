# Security research replication demos

This directory contains five deterministic, inspectable teaching demos inspired by the papers listed below. Each entry point is a standalone Python script. The default runs are simulator-free, use only the Python standard library, and do not require Docker, CARLA, Veins, an OCPP endpoint, or charging hardware.

These are scoped replications of research ideas, not byte-for-byte reproductions of the authors' experimental systems or published numerical results. Each section separates the paper-derived mechanism from the assumptions and METS-R-oriented consequence metrics added by the demo.

Run the commands from the repository root. Use only synthetic data or systems you own and are authorized to test; see [Safe use](#safe-use).

## Quick start

Each command below generates machine-readable results and a standalone HTML dashboard in `output/security_demos/`.

```bash
python tutorials/security_demos/demo_01_location_spoofing.py
```

```bash
python tutorials/security_demos/demo_02_cav_bottleneck.py
```

```bash
python tutorials/security_demos/demo_03_trajectory_detector.py
```

```bash
python tutorials/security_demos/demo_04_emuocpp_consequences.py
```

```bash
python tutorials/security_demos/demo_05_chargex_state_manipulation.py --attack-mode serial_dos
```

Pass `--help` to any script for all parameters. The main controls are:

| Demo | Entry point | Main controls |
| --- | --- | --- |
| 1 | `demo_01_location_spoofing.py` | `--seed`, `--duration`, `--fleet`, `--requests`, `--spoofer-budget`, `--objective {delay,failure}`, `--output-dir` |
| 2 | `demo_02_cav_bottleneck.py` | `--seed`, `--duration`, `--arrival`, `--bias`, `--delay`, `--output-dir` (`--output` is an alias) |
| 3 | `demo_03_trajectory_detector.py` | `--input-csv`, synthetic train/test sizes, `--points`, `--epochs`, `--seed`, `--output-dir` |
| 4 | `demo_04_emuocpp_consequences.py` | fleet/station sizing, attack window, duplicate-ID policy, flood/capacity controls, `--seed`, `--output-dir` |
| 5 | `demo_05_chargex_state_manipulation.py` | `--attack-mode`, fleet/station sizing, attack window, pilot-equation and fallback controls, `--seed`, `--output-dir` |

## What each demo reproduces

### 1. Location spoofing against an autonomous fleet

Entry point: [`demo_01_location_spoofing.py`](demo_01_location_spoofing.py)

Primary reference: Jinghan Yang, Andrew Estornell, and Yevgeniy Vorobeychik, “Location Spoofing Attacks on Autonomous Fleets,” *VehicleSec 2023*, DOI [10.14722/vehiclesec.2023.23031](https://doi.org/10.14722/vehiclesec.2023.23031). See the [official paper page](https://www.ndss-symposium.org/ndss-paper/auto-draft-371/) and [paper PDF](https://www.ndss-symposium.org/wp-content/uploads/2023/02/vehiclesec2023-23031-paper.pdf).

Paper-derived elements:

- A limited spoofing-device budget and separate delay-maximization and service-failure objectives.
- Static-static and dynamically repositioned/re-targeted attack strategies.
- Spoofing changes a taxi's perceived location before fleet matching and routing, so the effect can propagate to system-level service.

Demo assumptions and extensions:

- A deterministic 9-by-9 synthetic grid, lightweight dispatch/routing rules, and transparent greedy attack strategies replace the paper's real road network, optimization formulation, and learned dynamic policy.
- The script models the consequence of an already-successful location spoof. It neither generates a GPS signal nor communicates with a vehicle.
- Passenger wait, correct completions, wrong-zone drop-offs, fleet mileage, and zone-level service loss are teaching metrics. They are not claimed to reproduce the paper's reported values.

### 2. Cyberattacks on cooperative CAV control at a bottleneck

Entry point: [`demo_02_cav_bottleneck.py`](demo_02_cav_bottleneck.py)

Primary reference: H. M. Sabbir Ahmad, Ehsan Sabouni, Wei Xiao, Christos G. Cassandras, and Wenchao Li, “Evaluations of Cyberattacks on Cooperative Control of Connected and Autonomous Vehicles at Bottleneck Points,” *VehicleSec 2023*, DOI [10.14722/vehiclesec.2023.23082](https://doi.org/10.14722/vehiclesec.2023.23082). See the [official paper page](https://www.ndss-symposium.org/ndss-paper/auto-draft-386/) and [paper PDF](https://www.ndss-symposium.org/wp-content/uploads/2023/02/vehiclesec2023-23082-paper.pdf).

Paper-derived elements:

- A CAV control zone at a shared bottleneck, with the paper's time step, acceleration/speed limits, and rear-gap barrier quantity.
- Paired baseline, slow-Sybil, constant-position-bias false-data-injection, and delayed-state scenarios.
- Safety margin, braking/control effort, throughput, traversal delay, queue, and telemetry-latency observations.

Important timing-versus-jamming correction: the paper's abstract and threat taxonomy mention jamming, but the third evaluated simulation in Section IV-C sends CAV 2 a three-second-old state for CAV 1. That is a timing/stale-message attack. This demo therefore implements an exact replay delay (three seconds by default), not RF jamming, interference, or packet loss. Message loss remains zero by construction and must not be interpreted as a jamming experiment.

Demo assumptions and extensions:

- The transparent controller is a CBF-inspired rear-gap clamp plus a FIFO merge reservation. It is not the paper's decentralized optimal-control program or proof implementation.
- The geometry is a two-lane merge; intersections, roundabouts, radios, RSUs, and a packet-level network are outside the standalone model.
- The demand generator and queue definitions are explicit teaching choices, so the output is comparative evidence for this approximation rather than a reproduction of the paper's plots.

### 3. Detection of falsified vehicle trajectories

Entry point: [`demo_03_trajectory_detector.py`](demo_03_trajectory_detector.py)

Primary reference: Jun Ying, Yiheng Feng, Qi Alfred Chen, and Z. Morley Mao, “WIP: Modeling and Detecting Falsified Vehicle Trajectories Under Data Spoofing Attacks,” *VehicleSec 2024*, DOI [10.14722/vehiclesec.2024.23035](https://doi.org/10.14722/vehiclesec.2024.23035). See the [official paper page](https://www.ndss-symposium.org/ndss-paper/auto-draft-469/), [paper PDF](https://www.ndss-symposium.org/wp-content/uploads/vehiclesec2024-35-paper.pdf), and the paper's referenced [VASP repository](https://github.com/quic/vasp).

Paper-derived elements:

- The complete-trajectory feature/classifier pipeline and the 13 transportation- and vehicle-domain features.
- Training on modeled known attacks and evaluating transfer to attack families absent from training.
- The paper's sample counts and SVM result are retained only as labeled context in `metrics.json`; they are not pass/fail targets for this demo.

Demo assumptions and extensions:

- Synthetic benign, ETA-delay, multi-sensor-fusion-drift, and unseen-attack trajectories replace VASP unless `--input-csv` is supplied.
- A standard-library logistic classifier replaces the paper's SVM, decision-tree, and random-forest evaluation.
- The prefix experiment is a demo extension. It reports survivor-only time from trace start to first alert among detected attacked trajectories, together with detected/eligible counts; it has no attack-onset field and is not attack-onset detection latency. The paper's reported detector consumes complete trajectories, so this timing metric must not be presented as a paper result.
- For external CSVs, attack families represented in training are treated as known and test-only families as unseen. If a test set has no supported known or unseen family, that group is marked unsupported in JSON and omitted from the dashboard rather than shown as a zero rate.

See [Using external trajectories](#using-external-trajectories-demo-3) for the CSV contract.

### 4. Operational consequences of selected EmuOCPP findings

Entry point: [`demo_04_emuocpp_consequences.py`](demo_04_emuocpp_consequences.py)

Primary reference: Soumaya Boussaha, Victor Fresno Gómez, Thomas Barber, and Daniele Antonioli, “EmuOCPP: Effective and Scalable OCPP Security and Privacy Testing,” *3rd USENIX Symposium on Vehicle Security and Privacy (VehicleSec 25)*, pages 81–97. See the [official paper page](https://www.usenix.org/conference/vehiclesec25/presentation/boussaha), [paper PDF](https://www.usenix.org/system/files/vehiclesec25-boussaha.pdf), and [authors' EmuOCPP repository](https://github.com/vfg27/EmuOCPP).

Paper-derived elements:

- Three paper findings used as scenario anchors: I1 charging-station impersonation through duplicate identifiers, M4 security-profile downgrade, and D1 resource exhaustion through unauthenticated `BootNotification` connection/authentication attempts.
- Paper-facing observations such as duplicate-ID session handling, connected legitimate stations, the lowest security profile reached, and loss of CSMS connection/authentication availability.

Demo assumptions and METS-R extensions:

- This is a deterministic state machine, not EmuOCPP. It sends no OCPP traffic, opens no attacker socket, manipulates no certificate, and performs no ARP/NDP spoofing or exploit workflow.
- The false `Unavailable` reports attached to the downgrade scenario, finite request-processing capacity, boot queue/backlog, and backlog threshold are transparent demo mechanisms. The paper does not report that false-status consequence or a measured backlog threshold.
- Taxi charging demand, station occupancy, retries, passenger service, fleet availability, charging failures, and downtime are METS-R-style operational consequences. The paper did not measure those fleet metrics.
- Duplicate-ID policy, request load, attack window, station capacity, and charging rates are exposed assumptions. Use the authors' code and an isolated authorized lab to reproduce protocol behavior.

### 5. ChargeX/J1772 charging-state manipulation consequences

Entry point: [`demo_05_chargex_state_manipulation.py`](demo_05_chargex_state_manipulation.py)

Primary reference: Ce Zhou, Qiben Yan, Zhiyan Yu, Eshan Dixit, Ning Zhang, Huacheng Zeng, and Alireza Safdari Ghanhdari, “Short: Breaking the Charge: Exploiting State Manipulation in EV Charging,” *3rd USENIX Symposium on Vehicle Security and Privacy (VehicleSec 25)*, pages 99–108. See the [official paper page](https://www.usenix.org/conference/vehiclesec25/presentation/zhou), [paper PDF](https://www.usenix.org/system/files/vehiclesec25-zhou.pdf), and [authors' demonstration site](https://chargerattacks.github.io/).

Paper-derived elements:

- SAE J1772 control-pilot state thresholds and the serial/parallel attachment equations used to calculate divergent EVSE/EV state observations.
- Software abstractions of serial denial of service, parallel forced-output, parallel error-state latching, and simulated unplug/replug recovery.
- The forced-output metric is based on the paper's approximately 6 A by 211 V station-side observation.

Demo assumptions and METS-R extensions:

- This is a safe software state machine. It does not drive pilot circuitry, control charging hardware, or provide hardware construction instructions.
- Fleet size, passenger demand, charger power, attack window, battery-use model, timeout, replug, and rerouting policy are explicit demo assumptions.
- Passenger wait/service, station queues, taxi downtime, battery shortfalls, and recovery are METS-R-style consequence metrics that the paper did not evaluate.
- Forced-output exposure is station-side energy exposure only and is never credited to the modeled battery. Neither the paper nor this demo establishes overcharging, battery degradation, or battery damage; do not make those claims from these outputs.
- `recovery_after_attack_min` is numeric only when recovery is observed before the simulation horizon. Otherwise it is `null` and `recovery_observed` is false, so a horizon-censored run is never presented as instantaneous recovery.

## Outputs and dashboards

Default artifact directories and filenames are:

| Demo | Default directory | Files |
| --- | --- | --- |
| 1 | `output/security_demos/location_spoofing/` | `results.json`, `scenario_metrics.csv`, `zone_service_loss.csv`, `dashboard.html` |
| 2 | `output/security_demos/cav_bottleneck/` | `results.json`, `scenario_metrics.csv`, `time_series.csv`, `attack_events.csv`, `dashboard.html` |
| 3 | `output/security_demos/trajectory_detector/` | `metrics.json`, `trajectories.csv`, `trajectory_features.csv`, `predictions.csv`, `dashboard.html` |
| 4 | `output/security_demos/emuocpp_consequences/` | `metrics.json`, `timeline.csv`, `events.csv`, `dashboard.html` |
| 5 | `output/security_demos/chargex_state_manipulation/` | `metrics.json`, `timeline.csv`, `events.csv`, `dashboard.html` |

The HTML files embed their plotted data and rendering code, so they remain useful without an internet connection. JSON contains scenario summaries, parameters, fidelity notes, and provenance; CSV files carry the detailed scenario, trajectory, time-series, event, feature, or prediction tables. Treat the files together as the complete run artifact.

To serve a finished dashboard on the loopback interface and ask the default browser to open it, add both flags:

```bash
python tutorials/security_demos/demo_02_cav_bottleneck.py --serve --open-browser
```

`--serve` keeps a local HTTP server running until `Ctrl-C`. `--open-browser` requests browser launch. All demos also accept `--host` and `--port`; port `0` selects an available port. Keep the default `127.0.0.1` binding unless an explicitly authorized environment requires otherwise.

## Using external trajectories (Demo 3)

Pass a long-form CSV in which each row is one timestamped point and every trajectory uses a stable `trajectory_id`:

```bash
python tutorials/security_demos/demo_03_trajectory_detector.py --input-csv path/to/trajectories.csv
```

Required columns:

| Column | Meaning and expected unit |
| --- | --- |
| `trajectory_id` | Stable string identifier; repeated for all points in one trajectory |
| `t` | Time in seconds |
| `x`, `y` | Position in metres in one consistent coordinate system |
| `speed` | Speed in metres per second |
| `acceleration` | Longitudinal acceleration in metres per second squared |
| `heading` | Heading in radians |
| `lead_gap` | Gap to the lead vehicle in metres |
| `label` | Benign/attacked label |

Optional columns:

| Column | Behavior |
| --- | --- |
| `attack_type` | Family label; defaults to `attack` or `benign` from `label` |
| `split` | `train` or `test`; when absent, a deterministic seed-controlled 70/30 assignment is made per trajectory |
| `map_profile` | Domain/map label; defaults to `external` |

Truthy attack labels are `1`, `true`, `attack`, `attacked`, `malicious`, or `yes`, case-insensitively. Other values are treated as benign. A file needs at least four trajectories, at least three timestamped points per trajectory, both train and test splits, both benign and attacked trajectories in training, and at least one benign and one attacked trajectory in test so detection and false-positive rates both have support. Explicit splits are strongly recommended for repeatable comparisons.

Minimal shape example (add more points and trajectories before running):

```csv
trajectory_id,t,x,y,speed,acceleration,heading,lead_gap,label,attack_type,split,map_profile
benign_train_1,0.0,0.0,0.0,12.0,0.0,0.0,28.0,0,benign,train,nyc
benign_train_1,0.1,1.2,0.0,12.0,0.0,0.0,28.1,0,benign,train,nyc
benign_train_1,0.2,2.4,0.0,12.1,1.0,0.0,28.0,0,benign,train,nyc
```

The loader sorts points by `t`, and exports are canonicalized by `trajectory_id` then time so a reload/re-export is byte-stable. It does not infer units, resample traces, transform coordinates, or create trustworthy ground-truth labels.

## Handoff to METS-R, CARLA, and Veins

The five scripts do **not** start METS-R or claim that their abstract attack mechanisms already exist in the simulator. A concrete integration should retain an untouched baseline, fix the seed/input schedule, inject only the authorized state transformation, and record both simulator truth and the data visible to the consumer.

1. Fleet location spoofing: start from [`configs/run_interactive_NYC_blank.json`](../../configs/run_interactive_NYC_blank.json) and the lifecycle helpers `read_run_config`, `prepare_sim_dirs`, and `run_simulation_in_docker` in [`utils/util.py`](../../utils/util.py). Use `METSRClient.query_taxi`, `query_available_taxis`, and `query_pending_requests` for observations, `add_taxi_requests` for paired demand, and `dispatch_taxi` for assignments. `set_attack_vehicle` can label a controlled vehicle, but a new adapter is still required to keep true position separate from the perceived position passed to matching/routing.
2. CAV bottleneck: use [`configs/run_cosim_CARLAT5.json`](../../configs/run_cosim_CARLAT5.json) with `query_vehicle`, `generate_trip_between_roads`, `set_cosim_road`, `control_vehicle`, and `tick`. For a network-aware study, layer [`configs/run_v2x_veins_Template.json`](../../configs/run_v2x_veins_Template.json) and `VeinsClient.sync_tick` from [`clients/VeinsClient.py`](../../clients/VeinsClient.py). Implement the timing case as a controller-side queue that delivers a timestamped state after the selected delay. Measured latency is not evidence of RF jamming.
3. Trajectory detector: sample `METSRClient.query_vehicle(..., transform_coords=True)` on a fixed tick and export the [Demo 3 schema](#using-external-trajectories-demo-3). Alternatively, normalize BSM records from [`clients/KafkaDataProcessor.py`](../../clients/KafkaDataProcessor.py) or a `VeinsClient.sync_tick` trace. Attack labels and train/test membership must come from the experiment design, not from the detector's predictions.
4. EmuOCPP consequences: run the authors' EmuOCPP tooling only in an authorized isolated lab, reduce its results to a timestamped outcome schedule, and feed that schedule to a disposable METS-R experiment. `query_chargingStation`, `query_taxi`, and `go_charging` in [`clients/METSRClient.py`](../../clients/METSRClient.py) can measure simulated operational consequences. METS-R has no native OCPP session, certificate, boot-registry, or security-profile API, so an explicit adapter is required; do not treat station add/remove controls as protocol exploits.
5. ChargeX consequences: METS-R has no SAE J1772 pilot-line interface. Generate only a safe external state schedule, then use `query_chargingStation`, `query_taxi`, and `go_charging` to study simulated queue and service consequences. Hardware validation and battery-safety conclusions are outside this integration.

For all integrations, record the repository revision, run configuration, random seeds, exact attack/fallback window, simulator versions, and raw observations. Compare paired runs before interpreting deltas, and label every metric as paper-replicated, demo-derived, or METS-R-derived.

## Safe use

- Run these demos locally or in isolated, explicitly authorized research infrastructure.
- Do not aim spoofing, V2X, OCPP, or charging-state experiments at public infrastructure, production fleets, real vehicles, or chargers without written authorization and an approved safety plan.
- The scripts model outcomes; they are not exploit tools, hardware guides, or evidence that a physical target is vulnerable.
- Keep dashboard serving on loopback by default. Review generated artifacts before sharing because imported traces may contain sensitive location or operational data.
- Follow the cited authors' disclosure guidance, your institution's research-ethics requirements, applicable law, and equipment/vendor safety procedures.
