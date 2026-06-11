# METS-R Veins Bridge

This folder contains a minimal OMNeT++ bridge process for `clients.VeinsClient`.
It listens for METS-R JSON-lines requests on a TCP port, accepts `sync_tick`
messages, and returns delivered messages plus latency/link metrics.

The current implementation is intentionally small: it runs inside OMNeT++,
hands each `sync_tick` request to the OMNeT++ event loop, schedules packet
delivery events, and reports latency as receive simulation time minus generation
simulation time. The bridge now exposes named backend profiles
(`abstract_omnetpp`, `veins_80211p`, and `sim5g_cellular`), but all three are
currently parameter profiles on the same abstract queue/contention model. They
do not yet instantiate full Veins 802.11p or Simu5G cellular PHY/MAC modules.
The stable Python protocol is the boundary where those deeper backends can be
connected later without changing the tutorial shape.

## Build In WSL

```bash
export OMNETPP_HOME=~/src/omnetpp-6.1
export VEINS_HOME=~/src/veins-veins-5.3.1
source "$OMNETPP_HOME/setenv"

cd ~/src/METS-R_HPC/veins_bridge/omnetpp
bash ./build.sh
find out -name '*metsr_veins_bridge*'
```

## Start The Bridge

Run this in WSL and leave it open:

```bash
export OMNETPP_HOME=~/src/omnetpp-6.1
source "$OMNETPP_HOME/setenv"

cd ~/src/METS-R_HPC/veins_bridge/omnetpp
opp_run -u Cmdenv -n . -l ./out/clang-release/metsr_veins_bridge omnetpp.ini
```

Expected output includes a line like:

```text
METS-R Veins bridge listening on 0.0.0.0:9099
```

### Choose A Backend Profile

The default `[General]`/`[Config AbstractOmnetpp]` profile is the lightweight
event-scheduled model:

```bash
opp_run -u Cmdenv -n . -l ./out/clang-release/metsr_veins_bridge -c AbstractOmnetpp omnetpp.ini
```

To run the 802.11p-like profile:

```bash
opp_run -u Cmdenv -n . -l ./out/clang-release/metsr_veins_bridge -c Veins80211p omnetpp.ini
```

To run the cellular profile reserved for future Simu5G integration:

```bash
opp_run -u Cmdenv -n . -l ./out/clang-release/metsr_veins_bridge -c Sim5gCellular omnetpp.ini
```

The profile name appears in `hello`, `sync_tick`, per-message `link_metrics`,
and the tutorial CSV fields as `bridge_backend`. Today `veins_80211p` and
`sim5g_cellular` tune the current abstract model; their
`backend_implementation` values make that explicit.

When the Python example connects, the bridge also logs JSON requests, for
example:

```text
METS-R Veins bridge request type=hello request_id=1
METS-R Veins bridge request type=sync_tick request_id=2 tick=0 vehicles=61 bsm_messages=600
```

If the generated library path differs, pass the library stem to `-l`, not the
full generated filename. OMNeT++ adds the leading `lib` and trailing `.so`
itself.

Examples:

- If `find out -name 'libmetsr_veins_bridge.so'` prints
  `out/clang-release/libmetsr_veins_bridge.so`, use
  `-l ./out/clang-release/metsr_veins_bridge`.
- If it prints `out/gcc-release/libmetsr_veins_bridge.so`, use
  `-l ./out/gcc-release/metsr_veins_bridge`.

If OMNeT++ reports that a declared NED package does not match the expected
package, make sure you are running from `veins_bridge/omnetpp` with `-n .`.
The bridge NED files are intentionally package-less because they live directly
in that directory.

If OMNeT++ reports that `simtime_t` cannot represent the configured time, the
simulation limit is too large for the active time resolution. The included
`omnetpp.ini` uses a 7-day limit, which is within the default OMNeT++ range and
is paced in wall-clock time by the bridge keep-alive. After changing bridge C++
code, rerun `bash ./build.sh` before starting `opp_run`.

Cmdenv status lines such as `** Event #...`, `Speed:`, and `Messages:` are
normal OMNeT++ progress reports. When the bridge is idle, `present: 1` and
`in FES: 1` usually mean only the bridge polling event remains scheduled.

## Run The Python Town 05 BSM Example

From the METS-R_HPC repository:

```bash
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json --metsr_port 4000 --ticks 5 --csv output/town05_bsm_summary.csv --message_csv output/town05_bsm_messages.csv --table_rows 24
```

The default scenario uses `clients.METSRClient` to query at least four active
METS-R vehicles, treats their coordinates as the CARLA Town 05 vehicle states,
and sends Basic Safety Messages between every origin/target pair. The terminal
table and `output/town05_bsm_messages.csv` include origin/target vehicle IDs,
vehicle locations, distance, BSM content, delivery status, and latency.

If your METS-R instance uses a different websocket port, replace `4000` with
that port or pass explicit vehicles:

```bash
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json --metsr_port 4000 --metsr_vehicle_ids 501,502,503,504 --metsr_private_flags true,true,true,true --ticks 5 --message_csv output/town05_bsm_messages.csv
```

For an offline bridge-only smoke test, use the static seed vehicles instead of
METS-R:

```bash
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json --vehicle_source town05_seed --ticks 5 --message_csv output/town05_bsm_messages.csv --table_rows 24
```

To inspect a few live message rows while the run is progressing, add
`--trace_messages`:

```bash
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json --metsr_port 4000 --ticks 5 --message_csv output/town05_bsm_messages.csv --trace_messages 3 --table_rows 24
```

### Run BSM Attack Variants

Most VASP-style semantic attacks are represented as intentional BSM mutations
in the Python example. For example, to make one sender report an offset
position:

```bash
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json --vehicle_source town05_seed --ticks 5 --bsm_attack position_offset --attack_sender_ids 501 --attack_position_offset_x_m 40 --message_csv output/town05_position_attack_messages.csv --table_rows 24
```

To inject DoS-style channel load, add extra attacked messages:

```bash
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json --vehicle_source town05_seed --ticks 5 --bsm_attack dos --attack_sender_ids 501 --attack_dos_messages 80 --message_csv output/town05_dos_messages.csv --table_rows 24
```

Supported `--bsm_attack` values are `position_offset`, `speed_offset`,
`heading_offset`, `acceleration_offset`, `fake_emergency_brake`,
`ghost_vehicle`, and `dos`. Message CSV rows include `attacked`,
`attack_type`, `attack_id`, transmitted BSM fields, and truth fields such as
`truth_x`/`truth_y` when a semantic value was modified.

## Optional Load/Distance Experiment

The older synthetic noise-load scenario is still available with `--scenario
noise`. To show how sender location influences latency in that scenario, sweep
the sender ring away from the target over time:

```bash
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json --scenario noise --noise_senders 60 --messages_per_sender 10 --ticks 100 --sender_radius_m 50 --radius_end_m 900 --csv output/veins_distance_sweep.csv --message_csv output/veins_distance_messages.csv
```

The tick-level CSV includes `sender_radius_m`, distance summaries, delivery
rate, and latency summaries. The per-message CSV includes `distance_m` and
`latency_ms`, so you can plot latency versus distance directly.

If the Python process runs on Windows and the bridge runs in WSL, `127.0.0.1`
usually works on recent WSL2 versions. If it does not, get the WSL IP:

```bash
hostname -I
```

Then run the Python example with `--host <WSL_IP>` or update
`configs/run_v2x_veins_Template.json`.

## Protocol Boundary

The bridge implements:

- `hello`
- `ping`
- `reset`
- `sync_tick`

`sync_tick` receives:

- `vehicles`
- `bsm_messages`
- `attacks`

Messages should include stable matching fields when available:

- `message_id`
- `sender_id`
- `receiver_id`
- `tx_time_s`
- `radio_mode`
- `payload_bytes`
- BSM semantic fields such as `x`, `y`, `speed_mps`, `heading_deg`

It returns:

- `received_bsms`
- `link_metrics`
- `attack_events`
- `bridge_backend`
- `backend_implementation`
- `network_model`
- `radio_access`

For the current latency/noise example, messages with `receiver_id` or
`target_vehicle_id` are treated as intended unicast traffic to the target
vehicle. Latency increases with offered load to that receiver and message
payload size. Delivered messages are scheduled as OMNeT++ events; the returned
`latency_ms` is measured from the simulated delivery event time.

The abstract scheduled-delay model is:

```text
scheduled_delay_ms = baseLatencyMs
                   + perMessageLatencyMs * receiver_queue_position
                   + perPayloadByteLatencyUs * payload_bytes / 1000
                   + payload_serialization_delay
                   + propagation_delay
                   + distanceLatencyUsPerM * distance_m / 1000
                   + sampled_mac_backoff
                   + sampled_jitter
```

Packet drops are sampled from the configured contention loss slope and
communication range, with an additional `distanceLossAtRange` term for the
abstract distance-sensitive model. Because backoff, jitter, and packet drops are
sampled by the OMNeT++ module, the default example should no longer return the
same latency for every delivered message.

The backend profiles in `omnetpp.ini` mainly change:

- `bridgeBackend`, `backendImplementation`, `radioAccess`, and `backendNote`
- baseline latency and queueing terms
- nominal bitrate, slot/backoff scale, range, jitter, and distance-loss terms

Use `veins_80211p` for a Veins/VASP-style 802.11p baseline profile and
`sim5g_cellular` for the cellular option. A future full-stack implementation
should keep these backend names but replace the abstract scheduling internals
with actual Veins or Simu5G modules.

## Backend Roadmap

The bridge now has an explicit backend dispatch point in
`MetsrVeinsBridge.cc`. The current supported implementations are still
abstract profiles:

- `abstract_event_profile`
- `abstract_profile_pending_full_veins`
- `abstract_profile_pending_simu5g`

The next real backend steps are:

1. Add a Simu5G/INET build target and NED network for `sim5g_cellular_uu`.
2. Map each METS-R vehicle record to a Simu5G UE mobility module every tick.
3. Inject BSM payloads into Simu5G UE applications and measure receive events.
4. Export Simu5G radio/network metrics such as serving gNB, CQI, SINR, BLER,
   HARQ count, scheduler delay, and handover state into `link_metrics`.
5. Add a separate `cv2x_pc5_sidelink` backend for direct V2V/RSU delivery.
   This needs sidelink resource pools, sensing/resource selection, collision
   accounting, interference, BLER, and broadcast/multicast delivery semantics.

Semantic attacks should stay in Python as BSM mutations. DoS and other
radio-resource attacks should be implemented by injecting real load into the
selected backend so the radio model decides the resulting latency and loss.
