# Scenic traffic scenarios

Start with [traffic_template.scenic](traffic_template.scenic). Its numbered sections
are the editable experiment specification: map, traffic flow, ego controller,
safety encounter, and optional security perturbation. Copy it **within this directory**, give it a new name, and change
the `param` values. Safety/security selectors and their example configuration are
commented out, so the active template specifies normal traffic and a PCLA ego.
Their Python implementations remain available through explicit parameter/CLI
overrides; uncomment only the options needed for an experiment. The shared
Scenic implementation is included in `traffic_template.scenic`; presets import
`Main` from that file. There are no separate helper `.scenic` modules.
These scenarios require the [METSRSim Scenic fork](https://github.com/Kv139/Scenic/tree/METSRSim).

## Parameters to change

| Parameter | Meaning |
| --- | --- |
| `backend` | `"cosim"` (default), `"carla"`, or `"metsr"`; only the selected simulator model is loaded. |
| `town` | CARLA town, e.g. `"Town05"` or `"Town06"`. Configure METS-R for the same town. |
| `map`, `xml_map` | Matching OpenDRIVE and SUMO files. Defaults follow `../../data/CARLA/<town>/facility/road/`. Use `localPath(...)` for paths relative to a Scenic file. |
| `address` | CARLA host. `demo2.py` detects the Windows host under WSL; direct Scenic runs need the actual address. |
| `num_commuters` | Total background vehicle spawn budget, excluding the ego and dedicated hazards. Zero means no background traffic. This is **not** a target number of simultaneously active vehicles. |
| `spawn_interval_s` | Seconds between arrivals. First arrival is at time zero; at most one vehicle per Scenic tick. Rounded up to whole ticks. Existing presets default to one arrival per tick. |
| `length`, `timestep` | Duration and control/synchronization step, in simulation seconds. The scenario continues to `length` after the spawn budget is exhausted. |
| `bubble_size` | METS-R/CARLA bubble radius in meters. In the CARLA hybrid preset it instead means Traffic Manager's full-physics radius. |
| `allow_bubble_spawns` | Co-simulation only: whether random commuter spawns may occur inside the ego's bubble. |
| `ego_controller` | Co-simulation: `"pcla"`, `"autopilot"`, or `"external"`. External control needs a Python runner such as demo4. |
| `pcla_agent` | A single installed PCLA selector, e.g. `"lav_fast"`, `"lbc_lb"`, `"simlingo_simlingo"`. Requires its dependencies and weights. |
| `pcla_route` | `None` generates PCLA waypoints from the ego's authoritative METS-R route. An explicit route XML should describe the same route/map. |
| `seed` | Experiment label and seed used by demo2. With the Scenic CLI, also set `-s` to seed actual sampling. |
| `safety_scenario` | `none`, `vehicle_interception`, `traffic_blockage`, `sudden_braking`, `cut_in`, `merge`, or `collision_avoidance`. |
| `security_scenario` | `none`, `sensor_spoofing`, `traffic_control_spoofing`, `adversarial_perception`, or `adversarial_driving`. |
| `test_parameters` | Nested placement, trigger, event, hazard, sensor, perception, traffic-control and driving settings; see below. |
| `attack_behavior` | `"none"` (default) or `"periodic_brake"`. The latter overrides executed control commands. |
| `attack_target` | `"ego"` or a commuter name such as `"car_0"`. A commuter can brake only while represented by a CARLA actor. |
| `attack_start_s`, `attack_duration_s`, `attack_period_s` | First attack time, duration, and time between starts. Duration must be positive and no longer than the period. All use simulation seconds. |
| `initial_pose` | Optional co-simulation ego pose `(x, y, heading)` in Scenic coordinates; `None` samples a road position. |
| `export_folder`, `run_name` | Co-simulation output location and filename prefix. demo2 supplies its own run-specific output paths. |

The annotated template uses 100 commuters, one every 0.5 seconds, over 30 seconds:
only **60 arrivals** fit before the run ends. For timestep `dt`, the effective
interval is `max(1, ceil(spawn_interval_s / dt)) * dt`. Reaching the commuter budget
does not terminate the experiment early. `length` also ends on a simulation tick.
A PCLA controller expecting 20 Hz needs `--timestep 0.05`. For a matched 20 Hz
co-simulation, also pass `--metsr-tick-seconds 0.05` and configure the METS-R server
for 0.05 seconds. The template retains the historical 0.1-second default; this is
10 Hz. The runner accepts integer tick ratios, but acceptance alone does not
establish correct clock advancement in every version of the Scenic adapter.
Use equal clocks for these experiments.
The number of successfully active vehicles can differ from requested arrivals
because of completed trips, road capacity, or simulator spawn failures.

## Run a co-simulation experiment

Run from the repository root in the environment containing Scenic, CARLA, and the
chosen PCLA dependencies. Start the required simulator servers first.

```bash
python tutorials/dashboard_demos/demo2.py scenic_exp/scenarios/traffic_template.scenic --town Town06 --num-commuters 100 --spawn-interval-s 0.5 --length 30 --pcla-dir /mnt/d/git/PCLA --pcla-agent lav_fast
```

To use autopilot without loading PCLA, add `--ego-controller autopilot`. To select
another paired map, use `--town Town05`, or supply both `--opendrive-map` and
`--sumo-map`. `--allow-bubble-spawns` / `--no-allow-bubble-spawns` control placement.

For a full-brake attack during `[5, 6)`, `[10, 11)`, etc. simulation seconds:

```bash
python tutorials/dashboard_demos/demo2.py scenic_exp/scenarios/traffic_template.scenic --num-commuters 100 --spawn-interval-s 0.5 --length 30 --pcla-dir /mnt/d/git/PCLA --pcla-agent lav_fast --attack-behavior periodic_brake --attack-target ego --attack-start-s 5 --attack-duration-s 1 --attack-period-s 5
```

For controller runtime comparisons, keep the map, traffic, attack settings and seed
sequence identical. `all` is interpreted by demo2; do not put it in a Scenic
`pcla_agent` parameter. Unsupported external stacks such as Autoware are excluded
from the native sweep, and other agents still need their weights/dependencies.

```bash
python tutorials/dashboard_demos/demo2.py scenic_exp/scenarios/traffic_template.scenic --benchmark-agent --pcla-agent all --pcla-dir /mnt/d/git/PCLA --total-simulations 3 --seed 33 --town Town06 --num-commuters 100 --spawn-interval-s 0.5 --length 30 --attack-behavior none
```

Benchmark mode uses a PCLA ego and disables the dashboard and online visualization.
See [demo2_benchmark.md](../../tutorials/dashboard_demos/demo2_benchmark.md) for the
CSV timing definitions and [demo2_analysis.ipynb](../../tutorials/dashboard_demos/demo2_analysis.ipynb)
for stacked runtime charts. With a command attack, PCLA inference still runs once
per tick; the attack replaces its output, so PCLA timing remains measurable.
Identical settings/seeds reproduce the sampling inputs, but different controllers
can take different paths and therefore change the bubble and later traffic placement.

**Override precedence:** Scenic `-p` / Python `params` overrides win over file
parameters. demo2 forwards its normal defaults too (e.g. 5 commuters, 10 seconds,
Town06); pass those flags explicitly when you want the template's example values.
Flow, controller, safety/security selectors and legacy attack flags are optional
and preserve file values when omitted. `--test-config` loads JSON with only
`safety_scenario`, `security_scenario`, and/or `test_parameters`. Command-line
selectors override JSON selectors, which override the Scenic file. A JSON
`test_parameters` dictionary replaces the file dictionary; omitted groups/keys
inherit the shared defaults below. Benchmark mode selects `ego_controller="pcla"`.

## Existing filenames remain usable

Presets set their backend, traffic budget, duration, and special options before
`from traffic_template import Main`. They retain their original defaults,
including one arrival per tick; the template itself uses 100 commuters over
30 seconds with a 0.5-second arrival interval. Common defaults include Town06,
a 0.1-second timestep, seed 33, and attacks off. Co-simulation defaults to a
100 m bubble with background spawns outside it; CARLA-only defaults to autopilot
without hybrid physics. Add a `param` before the import to customize a preset,
or supply CLI/API overrides. Co-simulation presets declare their controller
explicitly so demo2 can detect whether PCLA is required before compilation.

| Entry point | Backend and defaults |
| --- | --- |
| `traffic_template.scenic` | Annotated co-simulation example: PCLA ego, 100 m bubble, attacks off. |
| `constant_flow_n_bubble_n_attack.scenic` | Existing demo2 default: PCLA ego, 100 m bubble, 1,000 commuter budget, 10 seconds. Its historical name does not enable an attack. |
| `constant_flow_n_bubble_50.scenic` | Co-simulation autopilot, random spawns allowed in bubble. Preserves the previous **100 m** default despite the filename; set `bubble_size=50` for 50 m. |
| `constant_flow_n_all_carla.scenic` | Co-simulation with a 100,000 m bubble; still needs METS-R. All mapped traffic is intended to fall in CARLA's region. |
| `constant_flow_n_only_carla.scenic` | CARLA-only, Traffic Manager autopilot; no METS-R or PCLA. |
| `constant_flow_n_hybrid_carla.scenic` | CARLA-only hybrid physics, 50 m full-physics radius. This is not METS-R/CARLA co-simulation. |
| `constant_flow_n_all_metsr.scenic` | METS-R-only demand. No physical ego or PCLA; `num_commuters` counts all requested trips. |
| `town05_stop_sign_patch.scenic` | demo4's Town05 scenario: externally controlled ego, fixed start, paired local traffic and visual stop-sign patch. |

The old scripts' machine-specific addresses now default to localhost. Missing town
and duration defaults have been supplied. CARLA-only vehicles explicitly use
autopilot, and finite spawn budgets no longer stop the run before `length`.

`demo2.py` constructs a co-simulator, so use the Scenic CLI for the CARLA-only and
METS-R-only presets instead of passing them to demo2:

```bash
python -m scenic -S --2d --count 1 -s 33 -p seed 33 -p town Town06 -p num_commuters 50 -p spawn_interval_s 0.5 -p length 30 -p address 192.168.0.1 scenic_exp/scenarios/constant_flow_n_only_carla.scenic
python -m scenic -S --2d --count 1 -s 33 -p seed 33 -p num_commuters 50 -p length 30 scenic_exp/scenarios/constant_flow_n_all_metsr.scenic
```

In this Scenic fork, CARLA hybrid physics is enabled inside rendering setup;
keep `render=1` for that preset. METS-R-only accepts `metsr_host`, `metsr_port`,
`simTimestep`, `origin` and `destination` (`-1` chooses a random endpoint).
Its `town`/`map` parameters do **not** reload the server's road network: configure
METS-R itself for that network. That backend does not support the braking example.

## Safety encounters

All new profiles use the co-simulation backend. They work with a PCLA or autopilot
ego and add dedicated hazard vehicles independently of background traffic. The
CARLA-only, METS-R-only, and externally controlled demo4 presets reject these
selectors. Existing periodic-brake behavior and preset filenames remain supported.

| `safety_scenario` | Implemented encounter | Extra vehicles |
| --- | --- | --- |
| `none` | Normal flow and a sampled ego start. | 0 |
| `vehicle_interception` | Adjacent car follows its lane, then crosses into/across the ego lane and stops at the end of its crossing path. | 1 |
| `traffic_blockage` | Stationary cars occupy the ego lane and one same-direction adjacent lane from time zero. Other lanes may remain passable. | 2 |
| `sudden_braking` | Lead car follows a lane at the target speed, brakes during the event, then resumes. | 1 |
| `cut_in` | Adjacent car starts ahead of ego and joins its lane when triggered. | 1 |
| `merge` | Adjacent car starts half the configured gap behind ego and joins its lane when triggered. This models a lane merge, not a map-specific on-ramp. | 1 |
| `collision_avoidance` | One stationary lead car from time zero challenges the ego's braking/avoidance. | 1 |

Placement uses sufficiently long lane sections from the paired OpenDRIVE/SUMO
maps. Multi-lane cases require a mapped, same-direction neighbor. Select a road,
signed OpenDRIVE lane ID, or eligible site index to constrain sampling; no suitable
site produces a clear error. The ego retains its METS-R route, so route choice,
agent speed, trigger and gap determine whether an encounter occurs. The template
does not require a collision or successful avoidance. Use short runs and inspect
the trajectories when tuning a new site.

Hazards start inside the bubble. `bubble_size` must be at least `gap_m + 15`;
keep it large enough for the entire encounter. Hazards use a simple path follower
with bounded steering/throttle/braking, and can only execute it while represented
in CARLA. They follow normal METS-R routing outside CARLA. `traffic_blockage`
blocks two lanes, rather than every lane of a highway. The gap follows the lane
centerline, so center-to-center distance on a curve can be smaller.

```bash
# Lead vehicle brakes once ego approaches within 20 m, after time 1 s.
python tutorials/dashboard_demos/demo2.py scenic_exp/scenarios/traffic_template.scenic --ego-controller autopilot --num-commuters 20 --length 30 --test-config scenic_exp/scenarios/configs/sudden_braking.json

# Change only the encounter; the other JSON values still apply.
python tutorials/dashboard_demos/demo2.py scenic_exp/scenarios/traffic_template.scenic --ego-controller autopilot --test-config scenic_exp/scenarios/configs/sudden_braking.json --safety-scenario cut_in --length 30
```

## Security perturbations

| `security_scenario` | Implemented effect and customization | Controller |
| --- | --- | --- |
| `sensor_spoofing` | Bias one declared speedometer, GNSS or IMU payload immediately before PCLA inference. Select its kind, optional sensor ID, and offsets. | PCLA |
| `traffic_control_spoofing` | Set one CARLA traffic light to Red/Yellow/Green/Off each active tick. Explicit actor ID or nearest light within a radius. | PCLA or autopilot |
| `adversarial_perception` | Overlay an RGB rectangle on one camera input, or black out its RGB channels. Select camera ID, image-relative ROI and color. | PCLA |
| `adversarial_driving` | Alter ego's executed control after inference: steering bias, sinusoidal weave, full brake, or throttle burst. | PCLA |

Choose one security effect and optionally combine it with a safety encounter.
These are synthetic perturbation mechanisms for testing robustness; the camera
patch is not optimized against a model and does not imply a successful attack.
Command corruption assumes compromised control output. For a malicious neighboring
driver, customize `SafetyHazard` instead. Sensor effects copy the affected payload
and preserve the frame identifier and other sensor feeds. A missing sensor type/ID
fails explicitly, since supported sensors differ between PCLA families.

Traffic-control spoofing changes the simulated CARLA light, not a METS-R signal
plan, network message, or controller-internal map. The nearest light need not govern
the ego's approach: choose a known actor ID and start position for a specific test.
If no light is in range, the run fails with an actionable error. The selected
light's prior state is restored at event end and teardown. Its full phase history
is not replayed. The implementation avoids `freeze()`, which freezes all traffic
lights in [CARLA 0.9.15](https://carla.readthedocs.io/en/0.9.15/python_api/#carlatrafficlight).

```bash
# Combine cut-in with a speedometer bias; PCLA inference still runs once per tick.
python tutorials/dashboard_demos/demo2.py scenic_exp/scenarios/traffic_template.scenic --pcla-dir /mnt/d/git/PCLA --pcla-agent lav_fast --test-config scenic_exp/scenarios/configs/cut_in_spoofed_speed.json --length 30

# Camera patch plus a stopped obstacle; install this agent's dependencies first.
python tutorials/dashboard_demos/demo2.py scenic_exp/scenarios/traffic_template.scenic --pcla-dir /mnt/d/git/PCLA --pcla-agent lav_fast --test-config scenic_exp/scenarios/configs/camera_patch.json --length 30

# Benchmark the same profile across supported native agents.
python tutorials/dashboard_demos/demo2.py scenic_exp/scenarios/traffic_template.scenic --benchmark-agent --pcla-agent all --pcla-dir /mnt/d/git/PCLA --test-config scenic_exp/scenarios/configs/adversarial_driving.json --total-simulations 3 --length 30
```

Add the matching clock flags described above for a 20 Hz controller. Example JSON
files also include [traffic_light_spoof.json](configs/traffic_light_spoof.json).
Edit the chosen file to customize it, or put the equivalent nested dictionary in
`param test_parameters`. Setting `--security-scenario none` disables the security
effect while retaining the JSON safety profile for a baseline trial.

## Playable configuration

Unknown keys and invalid ranges fail early. All times are simulation seconds,
distances are meters, and speeds are m/s.

| Group | Defaults and meaning |
| --- | --- |
| `placement` | `gap_m=25` (minimum 8); `road_id=null`, `lane_id=null`, `site_index=null` select eligible safety sites. Requires a safety profile; `initial_pose` is for security-only/normal traffic. |
| `trigger` | `mode="time"`, `start_s=3`, `distance_m=20`, `ttc_s=2`. Modes: time, distance, ttc. `start_s` is the earliest allowed onset in all modes. |
| `event` | `duration_s=2`, `repeat_s=0`. Zero repeat means one shot; otherwise repeat must be at least the duration. |
| `hazard` | `speed_mps=8`, `merge_distance_m=20`, `lookahead_m=5`. Target speed before/after braking, forward merge length, and path-following lookahead. Vehicles accelerate from rest. |
| `sensor` | `id=null`, `kind="speedometer"`, `speed_bias_mps=5`; `gnss_offset=[0.0001,0.0001,0]` (latitude degrees, longitude degrees, altitude m); `imu_offset=[0,0,0,0,0,0.1,0]` (accel xyz m/s^2, gyro xyz rad/s, compass rad). Speed is clamped to nonnegative. |
| `perception` | `camera_id=null`, `mode="patch"` or `"blackout"`; `roi=[0.4,0.4,0.2,0.2]` is normalized left/top/width/height; `color_rgb=[255,0,0]`. Camera payload is BGR/BGRA; conversion preserves alpha. |
| `traffic_control` | `actor_id=null`, `state="Green"`, `radius_m=80`. Null selects nearest light at each event onset; explicit IDs belong to the current CARLA world. |
| `driving` | `mode="weave"`, `steer_bias=0.15`, `weave_amplitude=0.3`, `weave_period_s=2`, `throttle=0.75`. Modes: weave, steering_bias, brake_check, throttle_burst. Steer is clamped to [-1,1]. |

Distance and TTC triggers require a safety hazard. TTC here is center distance
divided by positive radial closing speed, not a bounding-box collision prediction.
The first qualifying tick latches an onset; repeats use elapsed time from it.
Security uses the first qualifying hazard; moving hazards evaluate their own
encounter with ego. A trigger may never occur, which is observable in the records.
Static obstacles exist from t=0 regardless of trigger. Sudden braking and security
use the duration/repeat window. Cut-in, merge and interception change path once;
their motion is not reset at event end.

## Records and implementation notes

`Main` records `all_positions`, `all_velocities`, the legacy `attack_window`, and
`test_events`. Each test sample includes the sampled site ID, hazard onsets and
active flags, issued hazard-control counts, security onset/activity, modified
sensor/control/light counts, selected sensor/light ID, minimum center distance,
and minimum center-closing TTC. Counts confirm that the relevant hook issued an
effect; they do not certify its physical impact. A null onset or zero modification
count indicates an untriggered/unapplied test. These are diagnostics, not collision
or safety pass/fail metrics; add CARLA collision events and task-specific criteria
when evaluating outcomes.

Regular demo2 saves these records in its Scenic CSV. For enabled profiles, it also
writes a `*_test_events.json` report with resolved configuration and per-step
records. Benchmarks write `<agent>_run_<n>_test_events.json` beside the timing CSV,
after the timed simulation call. Different controllers can change later paths,
traffic and bubble membership even with identical initial sampling seeds.

Implementation locations:

- [traffic_template.scenic](traffic_template.scenic): sections 1-6 contain the
  editable parameters and commented safety/security examples; section 7 defines
  configuration validation, arrival scheduling, and periodic braking. Sections
  8-10 contain the co-simulation, CARLA-only, and METS-R-only implementations.
  Each backend defines `Main`; only the selected backend is loaded.
  `SafetyHazard` controls hazard motion, and `EgoController` owns runtime
  setup/cleanup and invokes `PCLAAgent` or autopilot.
- [utils/scenic_testing.py](../../utils/scenic_testing.py): `safety_sites` selects
  geometry; `merge_path` and `path_control` implement hazard motion;
  `trigger_reached`/`event_active` schedule effects; `ScenarioRuntime` owns the
  reversible sensor hook and light override; `perturb_sensor_batch` edits copied
  observations and `modify_control` alters executed commands.

To add a behavior, extend `SAFETY_SCENARIOS` or `SECURITY_SCENARIOS` and the relevant
config validation in the Python helper (demo2 imports these choices). Add its
placement in `safety_sites` and its motion in `SafetyHazard`, or add a runtime effect
with restoration in `ScenarioRuntime.close`. Keep PCLA construction outside the
step loop and inference once per tick. Keep `self.pcla` for profiling and cleanup.
Scenic behaviors must yield using `take`, `wait` or `do` each iteration; imports
needed at runtime belong in regular Python functions in this fork.

Only issue hazard controls while `carla_actor_flag` is true and `carlaActor` exists.
Preserve METS-R routing outside the bubble. The co-simulation autopilot action is
`SetAutoPilotAction`; the CARLA-only spelling is `SetAutopilotAction`. Do not mix
legacy ego braking with a new command attack if you want to attribute its effect;
legacy braking takes final precedence. For an arbitrary visual transformation,
replace the image-editing branch of `perturb_sensor_batch`, keeping frame identity,
shape/channel conventions and untouched feeds intact.

For world-anchored stop-sign patch trials, [demo4.py](../../tutorials/dashboard_demos/demo4.py)
remains the specialized implementation. It owns its controller/attack through
`ego_controller="external"`; new profile selectors must stay `none`. Its
`local_commuter_poses` tuples `(x,y,z,heading,road_id,lane_id)` override random flow
placement. Outside safety profiles, `initial_pose=(x,y,heading)` fixes ego. Convert
CARLA coordinates to Scenic as `(x,-y)` and heading `-radians(carla_yaw+90)`.

Offline regression tests (no running simulator servers required):

```bash
python -m unittest discover -s tests -p 'test_scenic_testing.py'
python -m unittest discover -s tests -p 'test_scenic_traffic_templates.py'
```

Use the PCLA/Scenic environment to exercise actual Scenic compilation, seeded
Town05/Town06 placement, behavior steps and failure cleanup. DummySimulator checks
issued actions and scheduling; validate vehicle dynamics and agent responses in a
live CARLA/METS-R run for your selected map, controller and server versions.
