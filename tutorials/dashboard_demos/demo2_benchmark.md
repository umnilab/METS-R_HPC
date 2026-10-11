# Benchmark PCLA controllers with demo2

Run from the same Python environment and with the same CARLA/METS-R servers
used for the normal Scenic demo. The METSRSim Scenic fork, PCLA checkout,
controller dependencies, and model weights must already be installed.

```bash
# One controller, three repetitions (seeds 33, 34, 35 by default).
python tutorials/dashboard_demos/demo2.py --benchmark-agent --pcla-agent lav_fast --pcla-dir /path/to/PCLA --total-simulations 3

# Every native controller in this PCLA checkout, using the same scenario/seeds.
python tutorials/dashboard_demos/demo2.py --benchmark-agent --pcla-agent all --pcla-dir /path/to/PCLA --total-simulations 3 --export-folder output/controller_benchmarks
```

The positional Scenic file and existing options such as `--town`, `--length`,
`--num-commuters`, `--seed`, `--timestep`, and `--pcla-route` still apply.
`--no-increment-seed` repeats the same seed. Selecting `all` also enables
benchmark mode if `--benchmark-agent` is omitted.

`all` reads the installed PCLA `agents.json`, expands the trained-model seeds
for TransFuser V4, CaRL, Roach, PlanT, and PlanT 2, and excludes Autoware, which
requires a separate ROS 2/Docker deployment. These model seed suffixes are
independent of the scenario's `--seed`. A missing agent/config/weight path
produces a `skipped` CSV row. LMDrive, MindDrive, and Orion also probe their
optional LAVIS, nuScenes, FlashAttention, similaritymeasures, and Diffusers
dependencies before scenario setup;
missing or incompatible imports produce a run-0 `skipped` row with the actual
import error. LMDrive uses its bundled LAVIS/vision-encoder paths. Its inference
imports omit the unused BLIP-Diffusion model/processors, avoiding old Diffusers'
removed `huggingface_hub.cached_download` dependency while retaining LMDrive's
driving-model registrations. This does not modify PCLA files or downgrade the
installed Hugging Face packages. These probes do not install packages or download models. Other initialization or runtime errors produce
`failed` rows and a traceback in that controller's log. The sweep continues
with the next controller, printing the error reason in the terminal. Before
each controller, a read-only CARLA check verifies RPC/world access and that the
requested town is installed. Each readiness RPC waits at most five seconds
(or `--carla-timeout-s`, if shorter). A failed check writes a run-0 failure CSV
and log, then stops the sweep; remaining controllers are not attempted.
Underlying exception causes, including world-load timeouts, appear in the CSV
and terminal as well as the log. Ensure one CARLA server owns all of its ports
(normally 2000-2002); launching a second instance does not recover a hung one.
Stop the existing server processes before restarting CARLA.
The benchmark does not install dependencies. PCLA model constructors may fetch
missing Hugging Face assets; provision the weights before measuring runtime.
Explicit single selectors still use PCLA's own selector handling. This can
resolve `autoware_v1`, but the current demo2 loop does not implement Autoware's
clock/control ownership contract; starting its Docker stack alone is insufficient.
See [Autoware integration effort](#autoware-integration-effort).

LMDrive checkpoint paths and nested MindDrive/Orion config paths beginning with
`pcla_agents/` or `./pcla_agents/` resolve against `--pcla-dir`. Absolute paths
and Hugging Face model IDs retain their meaning. This applies before model
construction without changing the process working directory or PCLA files.
LMDrive also needs its configured base LLM, in addition to the vision encoder
and driving checkpoint: `huggyllama/llama-7b`, `liuhaotian/llava-v1.5-7b`, or
`lmsys/vicuna-7b-v1.5-16k`. Use the corresponding Hugging Face cache or set
`llm_model` in its PCLA `lmdriver_config_*.py` to a local model directory.

For the current WSL PCLA environment, the following adds similaritymeasures
and updates Diffusers while retaining Hub 0.36.2, Transformers 4.46.3, and Torch
2.2.0 (dependency resolution was checked on 2026-09-13):

```bash
python -m pip install --no-deps "similaritymeasures==1.4.0" "diffusers==0.31.0"
```

Diffusers 0.31.0 avoids the removed Hub `cached_download` API and provides the
DDIM scheduler used by Orion. See `demo2.py` comments for FlashAttention and
nuScenes installation commands for this environment.

Large-model workers (LMDrive, MindDrive, Orion) default `PCLA_WATCHDOG_SEC` to
900 seconds, honoring any value already set in the environment. PCLA adds its
own one-second timer margin. Its watchdog raises `KeyboardInterrupt` on a
setup timeout even without keyboard input. Benchmark mode checks the watchdog
state and reports a confirmed setup timeout as `failed`, allowing the sweep to
continue. A keyboard interrupt without an expired watchdog still stops the
suite. For unusually slow loading, set a longer deadline explicitly:

```bash
PCLA_WATCHDOG_SEC=1800 python tutorials/dashboard_demos/demo2.py --benchmark-agent --pcla-agent lmdrive_llava --pcla-dir /path/to/PCLA
```

LMDrive's inference adapter also bounds its shuffled LiDAR source copy to the
40,000-point output buffer. This handles larger scans without a broadcast error
while retaining its ego-point filter, shuffle, sanitization, padding and rotation.
The correction is applied when loading the agent; PCLA source files are unchanged.

Each controller runs in a fresh Python process to isolate model imports and
GPU allocations. Each repetition reloads the CARLA world; Scenic's simulation
setup resets METS-R using its existing server configuration. Scenic and CARLA
Traffic Manager receive the same seed sequence for every controller. PCLA's
Python/NumPy random streams are isolated from Scenic's sampling. Controller
actions still change the resulting trajectories and bubble contents; this is
a comparison of the same scenario settings, not a replay of identical traffic.

CaRL/Roach and PlanT 2 also require a bird's-eye-view raster for the selected town. When
it is missing, benchmark mode generates it from the loaded CARLA map using
PCLA's map library at the model's configured resolution. The result is cached
inside the benchmark session; existing PCLA assets are left unchanged. This
preparation is included in `simulator_setup_s`, outside the throughput timing.
Missing PlanT speed-limit maps are prepared the same way, using the installed
PCLA algorithm against the current world. The standalone map scripts' server
startup and all-town reload loops are not executed. PlanT 2's nested BEV
observation managers use the cache for both the real and augmented ego vehicle.

Agent-local namespace packages are resolved before unrelated modules (for
example, PlanT's `util/` versus Scenic's `util.py`). Scenic route paths are
converted to strings for controllers that use environment variables. W&B is
disabled so unattended inference does not request credentials or upload logs.
ThinkTwice retains MMCV's module cache across repetitions so its classes are
not registered twice in MMEngine; each repetition still constructs a fresh
model and resets controller state.

PCLA throttle/brake values are bounded to [0, 1] and steering to [-1, 1] before
Scenic applies them. This accommodates LBC's raw PID commands; non-finite
outputs still fail. `pcla_clamped_action_calls` records how many actions needed
bounds applied, so this adjustment is visible in benchmark results.

## Controller inputs, outputs, and decision frequency

The table describes the installed PCLA adapters, checked against the
[tjleizeng/PCLA registry at revision `0b1727e`](https://github.com/tjleizeng/PCLA/blob/0b1727e/agents.json).
Variants and trained-model seeds share a row when their interfaces match.
Inputs include the adapter's sensors and navigation/state data; optional debug
cameras are omitted. **GT** means simulator ground truth; **BEV** means a
bird's-eye-view representation. GT-based policies have more information than
camera/LiDAR policies, so keep that distinction visible in comparisons.

For native PCLA agents, Scenic calls `get_action()` once per control tick:
`f = 1 / --timestep`. The default `--timestep 0.1` gives **10 action calls per
simulated second**. The last column describes fresh decisions after startup,
with numerical examples at that default. Repeated commands still count as
`pcla_action_calls`; that counter does not measure neural forward passes.

**T/B/S** denotes the final CARLA commands: throttle `[0, 1]`, brake `[0, 1]`,
and normalized steering `[-1, 1]`. Many models predict waypoints, paths, or
speed internally; their adapters convert those predictions to T/B/S before
Scenic applies them.

| Ego controller / PCLA selector | Input data | Output commands | Fresh decision frequency in this runner |
| --- | --- | --- | --- |
| [SimLingo](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/simlingo/agent_simlingo.py) `simlingo_simlingo` | Front RGB by default; GNSS, IMU, speed; route/target points and configured language prompt | T/B/S through path/speed tracking | Every call: `f` (**10 Hz**) |
| [LAV / LAV Fast](https://github.com/tjleizeng/PCLA/tree/0b1727e/pcla_agents/lav) `lav_lav`, `lav_fast` | RGB views including a telephoto view; LiDAR, GNSS, IMU, speed; route commands | T/B/S | Every call: `f` (**10 Hz**) |
| [LBC / World on Rails](https://github.com/tjleizeng/PCLA/tree/0b1727e/pcla_agents/wor) `lbc_*`, `wor_*` | Three RGB views (WOR adds a narrow front view), GNSS, speed; route commands | T/B/S | Every call: `f` (**10 Hz**) |
| [NEAT / AIM variants](https://github.com/tjleizeng/PCLA/tree/0b1727e/pcla_agents/neat) `neat_*` | RGB (NEAT uses front/left/right; AIM uses front), GNSS, IMU, speed; target point. Semantic/depth variant names describe training tasks, not required semantic/depth sensors | T/B/S | Every call: `f` (**10 Hz**) |
| [ThinkTwice](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/tt/thinktwice_agent.py) `tt_tt` | Four RGB views, LiDAR, GNSS, IMU, speed; navigation command/target point | T/B/S | Every call: `f` (**10 Hz**) |
| [TransFuser V3](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/transfuserv3/submission_agent.py) `tfv3_*` | Three RGB views, GNSS, IMU, speed; target point; LiDAR except camera-only `tfv3_ltf` | T/B/S | Default `action_repeat=2`: `f/2` (**5 Hz**); held command on alternate calls |
| [TransFuser V4](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/transfuserv4/sensor_agent.py) `tfv4_*` | Front RGB, GNSS, IMU, speed; route targets; LiDAR except `tfv4_aim_*` | T/B/S | Default `action_repeat=1`: `f` (**10 Hz**); a checkpoint setting of 2 halves this |
| [TransFuser V5](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/transfuserv5/sensor_agent.py) `tfv5_*` | Front RGB, LiDAR, GNSS, IMU, speed; route targets | T/B/S | Every call: `f` (**10 Hz**) |
| [TransFuser V6](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/transfuserv6/lead/inference/sensor_agent.py) `tfv6_*` | Configured RGB views, LiDAR, radar, GNSS, IMU, speed; route targets. See sensor/variant note below | T/B/S | Every call: `f` (**10 Hz**) |
| [CaRL / Roach](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/carl/eval_agent.py) `carl_carl_*`, `carl_carlv11`, `carl_roach_*` | **GT** semantic BEV from map, actors, traffic controls and route; ego motion and previous controls; no RGB/LiDAR observation pipeline | T/B/S; policy steering + signed acceleration are converted to steering/throttle/brake | `f/R`, normally `R=2`: **5 Hz**; see repeat rule below |
| [PlanT](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/plant/PlanT_agent.py) `carl_plant_*` | **GT** object boxes, route segments, traffic lights/stop signs and ego pose; IMU and speed | T/B/S through waypoint tracking | Every call: `f` (**10 Hz**) |
| [PlanT 2](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/plant2/PlanT_agent.py) `plant2_plant2_*` | **GT** object/route/traffic-control data and ego pose; IMU/speed; GT BEV when enabled by the checkpoint | T/B/S through path/speed tracking | Every call: `f` (**10 Hz**); initial brake frames precede driving |
| [InterFuser](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/interfuser/interfuser_agent.py) `if_if` | Three RGB views, LiDAR, GNSS, IMU, speed; navigation command/target point | T/B/S | Fresh command `f/2` (**5 Hz**), despite inference at `f` (**10 Hz**) with default `skip_frames=1` |
| [LMDrive](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/lmdrive/lmdriver_agent.py) `lmdrive_*` | Four RGB views, LiDAR, GNSS, IMU, speed; generated language instructions/target point; optional notices use GT traffic-light state | T/B/S through waypoint tracking | Alternate calls: `f/2` (**5 Hz**) |
| [MindDrive](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/minddrive/minddrive_agent.py) `minddrive_*` | Six RGB views, GNSS, IMU and speed, assembled into ego-state features; navigation command/target point | T/B/S | Every call: `f` (**10 Hz**) |
| [ORION](https://github.com/tjleizeng/PCLA/blob/0b1727e/pcla_agents/orion/orion_agent.py) `orion_base` | Six RGB views, GNSS, IMU and speed, assembled into ego-state features; navigation command/target point | T/B/S | Every call: `f` (**10 Hz**) |
| [Autoware](https://github.com/tjleizeng/PCLA/blob/main/pcla_agents/autoware/README.md) `autoware_v1` (separate integration required) | LiDAR, IMU, GNSS, vehicle state; Lanelet2 + pointcloud map + projector; goal/checkpoints; **GT** traffic-light injection. Adapter also spawns a camera, with camera light recognition disabled | Zenoh bridge writes CARLA `VehicleControl` directly; PCLA reads back T/B/S | ROS 2 nodes have separate timers. Kit expects a roughly **20 Hz simulator clock**; the README does not specify a single planning/control decision rate. `get_action()` is only a readback |

Rate and input details:

- CaRL/Roach use `R = action_repeat * (original_frame_rate // frame_rate)`
  unless `HIGH_FREQ_INFERENCE=1`, when `R = action_repeat`. The inspected
  checkpoint settings are `1 * (20 // 10) = 2`. This repeat factor does not
  automatically adapt to demo2's timestep.
- TransFuser V6's sensor helper requests LiDAR and radar even for selectors
  named `noradar` or `visiononly`. Inspect the loaded checkpoint and model
  flags to establish which streams the policy consumes; selector names alone
  do not describe sensor-generation cost. Optional visualization sensors can
  similarly contribute cost without being policy inputs.
- These are adapter schedules derived from code, not measured real-time
  performance. Sensor sampling rates, startup/buffer delays, ensembles, and
  repeated commands can all differ from neural inference frequency.
  `steps_per_wall_second` measures actual loop throughput; the benchmark does
  not currently record each model's fresh-inference count.
- Several agents assume a 20 Hz simulator in filters, timestamps, or temporal
  buffers. At `--timestep 0.05`, `f=20 Hz` and `f/2=10 Hz`; using the default
  0.1 does not retune those internals. For a matched 20 Hz setup, use
  `--timestep 0.05 --metsr-tick-seconds 0.05` and configure the METS-R server
  itself for 0.05-second ticks. Keep the same clock settings across paired runs.

Outside this PCLA benchmark, `traffic_template.scenic` also supports
`ego_controller="autopilot"` (CARLA Traffic Manager, using simulator map/actor
state to issue vehicle controls each synchronous CARLA tick) and
`ego_controller="external"` (the Python runner supplies the controller and its
schedule). They are modes of the template, not entries in `--pcla-agent all`.

## Autoware integration effort

**Moderate for its standalone Town01 demo; high for a reliable demo2/METS-R
benchmark integration.** This is an engineering assessment from the linked
[Autoware guide](https://github.com/tjleizeng/PCLA/blob/main/pcla_agents/autoware/README.md)
and the current driver contracts, not a tested integration result.

The supplied setup targets a Linux shell with an NVIDIA GPU, Docker and
NVIDIA container support, matching CARLA 0.9.16 server/client, and the
ROS 2 Jazzy / Autoware 1.8.0 / Zenoh 1.9.0 stack. Scripts build the images and
prepare Town01. A standalone smoke test is the useful first milestone:
from the PCLA root, run `bash pcla_agents/autoware/kit/setup_autoware.sh`,
start exactly one stack with `bash ~/start_stack.sh`, then run
`python sample_autoware.py` in the PCLA environment once it reports READY.

The main work before adding it to this benchmark is:

1. **Clock and control ownership.** The
   [Autoware sample](https://github.com/tjleizeng/PCLA/blob/main/sample_autoware.py)
   waits for bridge ticks; it must not call `world.tick()` or change synchronous
   mode. The bridge also applies ego controls directly. Scenic currently owns
   those operations. Integration must establish one tick owner, synchronize
   METS-R to that clock, and prevent competing ego commands.
2. **Ego and reset lifecycle.** Spawn the required `autoware_v1` role, coordinate
   the sensor kit and bridge, initialize localization/engagement, and use the
   supplied reset/cleanup helpers between repetitions and world reloads.
3. **Town06 maps and traffic lights.** The current template defaults to Town06.
   Generate its pointcloud map and provide Lanelet2/projector assets. The guide
   documents working traffic lights out of the box only for Town01; Town06
   needs traffic-light regulatory annotations. The current light input is GT,
   which must be disclosed in perception comparisons.
4. **Route and timing comparability.** Autoware plans to a destination, with
   optional checkpoints, rather than following every supplied route waypoint.
   Verify that its path matches the intended METS-R route. Its computation runs
   in Docker/ROS processes, so `pcla_action_s` measures control readback and
   cannot serve as Autoware inference latency; add ROS timing measurements.

## Output

The terminal prints a new session directory under
`<export-folder>/benchmarks/<session>/`. It contains:

- `<pcla_selector>.csv`: one row per attempted simulation, or a run-0 row for
  an unavailable controller or startup failure.
- `<pcla_selector>.log`: worker output, including dependency/runtime errors.
- `artifacts/<pcla_selector>/`: scenario-generated per-run files.
- `map_cache/`: generated bird's-eye-view and speed-limit maps, shared across model seeds.

CSVs are written before each run and after success/failure. Writes replace
the previous CSV only after the complete file has been written, so a failed
write leaves the previous result intact. The suite preserves extra columns
from workers with a newer CSV schema. Restart the command after code updates
to load fixes in the parent process as well as its workers. A native worker
crash leaves a failed row; earlier completed runs are retained. Ctrl+C stops
the sweep and records interruption. The command exits nonzero if a controller
fails or no controller completes. Missing-asset skips alone do not fail an
otherwise successful sweep. Re-running creates a new session directory.

## Timing definitions

No dashboard, visualization streaming, or extra dashboard sensors run in
benchmark mode. The controller's own CARLA sensors stay enabled. Built-in
Scenic/co-simulation recording and logging inside the loop remain included.

| CSV column | Meaning |
| --- | --- |
| `simulation_steps` | Completed Scenic steps, including on a partial failed run. |
| `simulation_seconds` | Completed steps multiplied by the simulation timestep. |
| `loop_wall_s` | Elapsed wall time in Scenic's main `_run` loop. |
| `steps_per_wall_second` | `simulation_steps / loop_wall_s`. |
| `realtime_factor` | `simulation_seconds / loop_wall_s`; 1 means real time. |
| `metsr_s` | Time in METS-R client methods, including serialization, network waits, and simulation response waits. |
| `carla_s` | Time in CARLA client/world/actor/map/Traffic Manager API methods, including blocking tick waits. |
| `pcla_agent_s` | Time in PCLA construction and `get_action()`, excluding nested measured simulator calls. Includes preprocessing and sensor waits. |
| `scenic_s` | Remaining loop time: behaviors, geometry, synchronization logic, recording, and instrumentation overhead. |
| `pcla_setup_s` | Inclusive PCLA construction time inside the loop, including its nested CARLA calls. |
| `pcla_action_s` | Inclusive time in PCLA `get_action()`, including nested simulator API calls. |
| `pcla_action_calls` | Number of attempted controller actions in the loop. A run with no observed PCLA actions fails validation. |
| `pcla_clamped_action_calls` | Actions with at least one finite command outside the actuator bounds; zero means no clipping was needed. |
| `metsr_tick_calls`, `carla_tick_calls` | API tick calls inside the loop; setup ticks outside the loop are excluded. These can differ from Scenic step counts. |
| `compile_s`, `scene_generation_s`, `simulator_setup_s` | Separately measured work outside the loop. Compilation happens once per controller process and is recorded on its first run. |
| `simulate_call_s` | Entire `simulate()` call, including simulation setup, main loop, finalization, and teardown. |

The four exclusive costs satisfy:

```text
loop_wall_s = metsr_s + carla_s + scenic_s + pcla_agent_s
```

`pcla_setup_s` and `pcla_action_s` are overlapping detail columns: **do not add
them to those four costs**. The default Scenic behavior creates the model on
its first step, so throughput includes that initial model load. Its cost is
reported explicitly instead of silently dropping the first step. Setup,
compilation, and teardown outside `_run` do not contribute to loop throughput.

These are client-observed wall-time costs, not server CPU time or CUDA kernel
profiles. Work on background sensor threads or on simulator/GPU processes is
reflected through blocking calls, not counted a second time as parallel work.
Runtime hooks are removed on exit; an incompatible CARLA/Scenic API produces
an explicit failure instead of an uninstrumented throughput result.

Integration boundaries follow [Scenic's METSRSim simulator](https://github.com/Kv139/Scenic/blob/METSRSim/src/scenic/simulators/cosim/simulator.py),
[PCLA's controller lifecycle](https://github.com/MasoudJTehrani/PCLA/blob/main/PCLA.py),
and the [PCLA selector resolver](https://github.com/MasoudJTehrani/PCLA/blob/main/pcla_functions/give_path.py).
