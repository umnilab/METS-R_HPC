# METS-R Veins Bridge

This folder contains a minimal OMNeT++ bridge process for `clients.VeinsClient`.
It listens for METS-R JSON-lines requests on a TCP port, accepts `sync_tick`
messages, and returns delivered messages plus latency/link metrics.

The current implementation is intentionally small: it runs inside OMNeT++,
hands each `sync_tick` request to the OMNeT++ event loop, schedules packet
delivery events, and reports latency as receive simulation time minus generation
simulation time. The wireless behavior is still an abstract queue/contention
model rather than a detailed Veins 802.11p/C-V2X PHY/MAC stack. It is the place
to connect that deeper Veins/INET radio stack next; the Python client and
tutorial do not need to change when that radio model is added.

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

## Run The Python Latency Example

From the METS-R_HPC repository:

```bash
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json \
  --noise_senders 60 --messages_per_sender 10 --ticks 100 --csv output/veins_latency.csv
```

To inspect the individual messages and link outcomes returned by the bridge,
write a per-message CSV or print a few sample rows:

```bash
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json \
  --noise_senders 60 --messages_per_sender 10 --ticks 20 \
  --csv output/veins_latency.csv --message_csv output/veins_messages.csv \
  --trace_messages 3
```

`output/veins_messages.csv` contains one row per intended BSM transfer with
sender, receiver, message count, distance, delivered/drop status, packet error
rate, and latency when delivered.

To show how sender location influences latency, sweep the sender ring away from
the target over time:

```bash
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json \
  --noise_senders 60 --messages_per_sender 10 --ticks 100 \
  --sender_radius_m 50 --radius_end_m 900 \
  --csv output/veins_distance_sweep.csv \
  --message_csv output/veins_distance_messages.csv
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

`sync_tick` receives `vehicles` and `bsm_messages`, then returns:

- `received_bsms`
- `link_metrics`
- `attack_events`

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
