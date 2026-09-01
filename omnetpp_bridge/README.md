# METS-R OMNeT++ V2X Bridge

This directory contains the packet-level V2X sidecar used by
`clients.VeinsClient`. It listens for JSON-lines requests on TCP port `9099`,
applies METS-R mobility updates, advances one selected network backend by the
requested `duration_s`, and returns received BSMs plus link and latency metrics.

The backends are separate implementations, not aliases for one radio model.
Start exactly one backend at a time.

## Choose a Backend

| Profile or runner | Implementation token | Best use | Fidelity boundary |
| --- | --- | --- | --- |
| `AbstractOmnetpp` | `abstract_event_profile` | Fast bridge, load, and distance regressions | Analytical queue/contention model; no packet PHY/MAC |
| `Veins80211p` | `veins_80211p_phy_mac` | ITS-G5/DSRC experiments | Real Veins 5.3.1 `Mac1609_4` and `PhyLayer80211p` receive events |
| `run_sim5g_uu.sh` | `simu5g_cellular_uu` | Infrastructure-mediated V2X | Simu5G NR Uu through UE, gNB, core network, and IP |
| `run_sim5g_pc5.sh` | `simu5g_lte_d2d_pc5` | Direct network-controlled LTE D2D multicast | Simu5G LTE D2D path; not complete C-V2X Mode 3/4 or NR-V2X Mode 2 |

METS-R simulates mobility directly, so the Veins backend does not use SUMO or
TraCI at runtime. A real-backend delivery is reported only after the receiver
application gets the packet from its network stack.

## Install the Toolchains in WSL

The commands below assume a fresh Ubuntu WSL shell and keep simulator
dependencies under `~/src`. Adjust the paths if the tools are already
installed.

Install the system packages needed to compile OMNeT++ and the bridge. These
steps build a headless `Cmdenv` install, which is enough for this bridge:

```bash
sudo apt update
sudo apt install -y \
    build-essential make diffutils pkg-config ccache clang lld gdb lldb \
    bison flex perl sed gawk python3 python3-pip python3-venv python3-dev \
    libxml2-dev zlib1g-dev doxygen graphviz xdg-utils libdw-dev \
    git wget unzip sumo sumo-tools
```

Download and build OMNeT++:

```bash
mkdir -p ~/src
cd ~/src
wget https://github.com/omnetpp/omnetpp/releases/download/omnetpp-6.1.0/omnetpp-6.1.0-linux-x86_64.tgz
tar xzf omnetpp-6.1.0-linux-x86_64.tgz

export OMNETPP_HOME=~/src/omnetpp-6.1
cd "$OMNETPP_HOME"
python3 -m venv .venv --upgrade-deps --clear --prompt "omnetpp/.venv"
source .venv/bin/activate
python3 -m pip install -r python/requirements.txt
python -m pip install 'setuptools<82'
python -c 'import pkg_resources; print("pkg_resources OK")'
source setenv
./configure WITH_QTENV=no WITH_OSG=no
make -j"$(nproc)"
```

Run `deactivate` after the build if the OMNeT++ Python environment is no longer
needed. In each new WSL shell, still source `OMNETPP_HOME/setenv` before using
OMNeT++ commands.

Install and build Veins 5.3.1 for the real 802.11p backend:

```bash
cd ~/src
git clone --branch veins-5.3.1 --depth 1 https://github.com/sommer/veins.git veins-veins-5.3.1

export VEINS_HOME=~/src/veins-veins-5.3.1
cd "$VEINS_HOME"
./configure
make -j"$(nproc)"
```

For Simu5G Uu or PC5, use the validated stack below:

- OMNeT++ 6.1
- INET 4.5.x
- Simu5G 1.4.4, tag `v1.4.4`, commit `5b67d3d`

Build INET and Simu5G using their release instructions, then export
`OMNETPP_HOME`, `INET_HOME`, and `SIMU5G_HOME` and source each project's
`setenv` file. `check_sim5g_env.sh` verifies the version and required LTE D2D
APIs before compilation.

```bash
export OMNETPP_HOME=~/src/omnetpp-6.1
export INET_HOME=~/src/inet4.5
export SIMU5G_HOME=~/src/Simu5G-1.4.4

source ${OMNETPP_HOME}/setenv
cd ${INET_HOME}
. setenv
cd ${SIMU5G_HOME}
. setenv
```

Do not substitute `simu5g-latest`. Set `METSR_ALLOW_UNTESTED_SIMU5G=1` only
after porting and validating another release.

Clone this repository into WSL if you have not already done so:

```bash
cd ~/src
git clone https://github.com/umnilab/METS-R_HPC.git METS-R_HPC
```

## Build and Start One Backend

In each new WSL shell, load OMNeT++ and enter the bridge directory:

```bash
export OMNETPP_HOME=~/src/omnetpp-6.1
source "$OMNETPP_HOME/setenv"

cd ~/src/METS-R_HPC/omnetpp_bridge
```

### Backend Commands

The default `[General]`/`[Config AbstractOmnetpp]` profile is the lightweight
event-scheduled model:

```bash
bash ./build.sh
bash ./run_abstract.sh
```

For a manual launch, use the isolated generated NED root rather than `-n .`:

```bash
opp_run -u Cmdenv -n .generated/abstract-ned \
  -l ./metsr_veins_bridge -c AbstractOmnetpp omnetpp.ini
```

To run the real Veins 802.11p PHY/MAC backend:

```bash
bash ./build_veins.sh
bash ./run_veins_80211p.sh
```

The generic build excludes `src/veins`. Veins-dependent NED definitions are
kept as `.ned.template` files under `veins/`; the abstract runner loads only
`.generated/abstract-ned`. The Veins build materializes its definitions under
`.generated/veins-ned`, generates `MetsrBsmMessage`, enables
`METSR_WITH_VEINS`, and links the Veins library.

To build and run the separate Simu5G Uu backend:

```bash
bash ./build_sim5g.sh
bash ./run_sim5g_uu.sh
```

This build compiles the bridge with the Simu5G UE app and METS-R external
mobility modules. `sync_tick` packets are injected into Simu5G UE applications
and completed from real receive events. PC5 has a separate generated network:

```bash
bash ./run_sim5g_pc5.sh
```

Start only one backend on port `9099`. A ready sidecar prints:

```text
METS-R Veins bridge listening on 0.0.0.0:9099
```

For real Simu5G results, `application_tx_time_s` retains the caller/Scenic
clock, while `bridge_tx_time_s` and `tx_time_s` use the OMNeT++ radio-injection
clock. `rx_time_s` and `latency_ms` use that same OMNeT++ clock, so latency
remains self-consistent even when Scenic and the sidecar have different time
origins.

The profile name appears in `hello`, `sync_tick`, per-message `link_metrics`,
and the tutorial CSV fields as `bridge_backend`. Check
`backend_implementation`: the real Veins path reports
`veins_80211p_phy_mac`, while the lightweight path reports
`abstract_event_profile`.

When the Python example connects, the bridge also logs JSON requests, for
example:

```text
METS-R Veins bridge request type=hello request_id=1
METS-R Veins bridge request type=sync_tick request_id=2 tick=0 vehicles=61 bsm_messages=600
```

The build emits `libmetsr_veins_bridge*.so` in `omnetpp_bridge`; `out/`
contains object files. When invoking `opp_run -l` manually, pass the library
stem (for example `./metsr_veins_bridge`), without `lib` or `.so`.

For `AbstractOmnetpp`, use `run_abstract.sh` or pass
`-n .generated/abstract-ned` manually. OMNeT++ recursively scans every NED
source folder, so `-n .` is intentionally avoided: it could also discover
artifacts generated for a different backend. The root bridge NED files are
package-less source inputs. Optional Veins NEDs are loaded only by
`run_veins_80211p.sh`, and Simu5G NEDs only by their corresponding run scripts.

If OMNeT++ reports that `simtime_t` cannot represent the configured time, the
simulation limit is too large for the active time resolution. The included
`omnetpp.ini` uses a 7-day limit, which is within the default OMNeT++ range.
Idle socket polling stays at a fixed OMNeT++ timestamp; only an active
`sync_tick` advances model time. After changing bridge C++ code, rerun
`bash ./build.sh` for `AbstractOmnetpp` (then `bash ./run_abstract.sh`) or
`bash ./build_veins.sh` for `Veins80211p` before starting `opp_run`.

Cmdenv status lines such as `** Event #...`, `Speed:`, and `Messages:` are
normal OMNeT++ progress reports. When the bridge is idle, `present: 1` and
`in FES: 1` usually mean only the bridge polling event remains scheduled.

## Validate with the Python Town 05 Example

From the METS-R_HPC repository:

```bash
python tutorials/v2x_veins_example.py -r configs/run_v2x_veins_Template.json --metsr_port 4000 --ticks 5 --csv output/town05_bsm_summary.csv --message_csv output/town05_bsm_messages.csv --table_rows 24
```

The default scenario uses `clients.METSRClient` to query at least four active
METS-R vehicles, treats their coordinates as the CARLA Town 05 vehicle states,
and sends one broadcast Basic Safety Message per origin. The terminal
table and `output/town05_bsm_messages.csv` include origin/target vehicle IDs,
vehicle locations, distance, BSM content, delivery status, and latency.

If your METS-R instance uses a different websocket port, replace `4000` with
that port or pass explicit vehicles:

```bash
python tutorials/v2x_veins_example.py -r configs/run_v2x_veins_Template.json --metsr_port 4000 --metsr_vehicle_ids 501,502,503,504 --metsr_private_flags true,true,true,true --ticks 5 --message_csv output/town05_bsm_messages.csv
```

For an offline bridge-only smoke test, use the static seed vehicles instead of
METS-R:

```bash
python tutorials/v2x_veins_example.py -r configs/run_v2x_veins_Template.json --vehicle_source town05_seed --ticks 5 --message_csv output/town05_bsm_messages.csv --table_rows 24
```

To inspect a few live message rows while the run is progressing, add
`--trace_messages`:

```bash
python tutorials/v2x_veins_example.py -r configs/run_v2x_veins_Template.json --metsr_port 4000 --ticks 5 --message_csv output/town05_bsm_messages.csv --trace_messages 3 --table_rows 24
```

### BSM Attack Variants

Most VASP-style semantic attacks are represented as intentional BSM mutations
in the Python example. For example, to make one sender report an offset
position:

```bash
python tutorials/v2x_veins_example.py -r configs/run_v2x_veins_Template.json --vehicle_source town05_seed --ticks 5 --bsm_attack position_offset --attack_sender_ids 501 --attack_position_offset_x_m 40 --message_csv output/town05_position_attack_messages.csv --table_rows 24
```

To inject DoS-style channel load, add extra attacked messages:

```bash
python tutorials/v2x_veins_example.py -r configs/run_v2x_veins_Template.json --vehicle_source town05_seed --ticks 5 --bsm_attack dos --attack_sender_ids 501 --attack_dos_messages 80 --message_csv output/town05_dos_messages.csv --table_rows 24
```

Supported `--bsm_attack` values are `position_offset`, `speed_offset`,
`heading_offset`, `acceleration_offset`, `fake_emergency_brake`,
`ghost_vehicle`, and `dos`. Message CSV rows include `attacked`,
`attack_type`, `attack_id`, transmitted BSM fields, and truth fields such as
`truth_x`/`truth_y` when a semantic value was modified.

## Load and Distance Sweep

The older synthetic noise-load scenario is still available with `--scenario
noise`. To show how sender location influences latency in that scenario, sweep
the sender ring away from the target over time:

```bash
python tutorials/v2x_veins_example.py -r configs/run_v2x_veins_Template.json --scenario noise --noise_senders 60 --messages_per_sender 10 --ticks 100 --sender_radius_m 50 --radius_end_m 900 --csv output/veins_distance_sweep.csv --message_csv output/veins_distance_messages.csv
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

## JSON-Lines Protocol

The bridge implements:

- `hello`
- `ping`
- `reset` (rejected with restart guidance; OMNeT++ time and radio state
  cannot be rewound safely in place)
- `sync_tick`

`sync_tick` receives:

- `duration_s` (finite and positive; defaults to `0.1`)
- `vehicles`
- `bsm_messages`
- `attacks`

Messages should include stable matching fields when available:

- `message_id`
- `sender_id`
- `receiver_id` (`-1` for a native broadcast)
- `expected_receiver_ids` (the receivers to match/report for a broadcast)
- `tx_time_s`
- `radio_mode`
- `payload_bytes`
- `wire_payload.data_b64`, `wire_payload.encoding`, and
  `wire_payload.bit_length` for an encoded payload
- BSM semantic fields such as `x`, `y`, `speed_mps`, `heading_deg`

It returns:

- `received_bsms`
- `link_metrics`
- `attack_events`
- `bridge_backend`
- `backend_implementation`
- `network_model`
- `radio_access`
- `duration_s`, `tick_start_time_s`, and `tick_end_time_s`

Each successful request completes at exactly `tick_start_time_s + duration_s`.
Radio receive timeouts are capped at that boundary. Packets that cannot finish
inside the tick are reported as drops instead of silently stretching the next
traffic step.

For a native broadcast, Veins performs one MAC transmission and the bridge emits
one link result for every expected receiver that actually receives that frame.
PC5 likewise performs one physical multicast. Uu has no native BSM broadcast in
this topology, so it expands the receiver set into explicit infrastructure
unicasts and labels each result `broadcast_delivery=expanded_to_uu_unicast`.
Legacy pairwise BSM rows are accepted for compatibility and consolidated only
when their wire bytes, message count, timing, content, and attack transform are
identical. The abstract backend expands a broadcast into logical link
evaluations, tagged with one shared `transmission_id`; those rows are not
separate physical sends.

The opaque base64 payload is stored in the actual `MetsrBsmMessage` packet. Its
decoded byte length controls PHY packet length, and each receive report carries
the base64 bytes, encoding, byte length, and bit length back to the bridge. A
byte mismatch is reported as `wire_payload_mismatch`, not as a delivery.
See [the strict J2735 ASN.1/UPER guide](j2735_uper.md) for schema,
mapping, dependency, and fail-fast configuration details.

`bridgeInjectionSpread` (20 ms by default) gives periodic BSM generators stable
phases before frames enter the real MAC. This avoids manufacturing a perfectly
synchronized TX collision when all vehicle rows arrive in one `sync_tick`.
Set `**.appl.bridgeInjectionSpread = 0s` to intentionally test that collision;
all contention, interference, and reception after generation remain Veins
PHY/MAC decisions. `latency_ms` starts at the reported physical injection
time; `bridge_injection_delay_ms` and `bridge_end_to_end_latency_ms` expose
the artificial phase separately.

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

The parameters below describe only `AbstractOmnetpp`:

- `bridgeBackend`, `backendImplementation`, `radioAccess`, and `backendNote`
- baseline latency and queueing terms
- nominal bitrate, slot/backoff scale, range, jitter, and distance-loss terms

`Veins80211p` does not use that delay/loss formula; Veins PHY/MAC receive events
determine delivery, latency, SNIR, receive power, and bitrate.

## Backend Technical Reference

The dispatch point in `MetsrVeinsBridge.cc` recognizes the implementation
tokens listed in the backend table. `simu5g_nr_d2d_pc5` remains accepted only
as a deprecated compatibility alias; shipped PC5 runs report
`simu5g_lte_d2d_pc5`.

### Simu5G generated files and UE capacity

`build_sim5g.sh` materializes NED, INI, and XML files under
`.generated/sim5g-ned` and compiles with `METSR_WITH_SIMU5G`. Keeping these
files outside the default source path prevents the abstract backend from
loading Simu5G when it is not installed.

Both Simu5G networks contain a fixed UE vector. The bridge keeps a stable
METS-R vehicle-ID-to-UE-index mapping for the lifetime of the sidecar, so the
default `64` is the maximum number of distinct IDs the process may observe,
not only the maximum simultaneous active count. Increase it before startup:

```bash
METSR_SIMU5G_NUM_VEHICLES=256 bash ./run_sim5g_pc5.sh
```

The same capacity override works with `run_sim5g_uu.sh`.

Restart the sidecar to clear the mapping. A larger vector increases
initialization and runtime cost.

### Infrastructure-mediated Uu

The Uu backend updates NR UE mobility, injects each BSM into the sender UE
application, and lets Simu5G/INET decide delivery through the gNB, core, and IP
path. The receiver UE application reports the result to `sync_tick`.

- V2V over Uu travels through cellular infrastructure, not direct sidelink.
- V2N or V2I may target a network, MEC, traffic-management, or infrastructure
  endpoint when such an application is added.
- More generally, Uu can represent V2X to an endpoint reachable through the
  cellular/IP network.

The checked-in Uu application implements UE-to-UE unicast and expands native
bridge broadcasts into one Uu unicast per expected receiver. It does not include
a separate MEC, MBMS/groupcast, or RSU relay application. Internal transport
IDs are generated by the bridge, so repeated caller `message_id` values cannot
overwrite pending deliveries.

### Network-controlled PC5 packet path

One bridge BSM creates one physical LTE `D2D_MULTI` transmission. Every UE that
successfully receives it reports an independent event:

The PC5 application does not silently fall back to Uu. At startup it requires
`cellularNic.d2dInitialMode=true` and a multicast destination.

```text
METS-R BSM
  -> MetsrBsmPc5App (one UDP/IP multicast packet)
  -> LteNicUeD2D / PDCP / RLC
  -> LteMacUeD2D D2D_MULTI buffer and eNB-controlled scheduling
  -> LtePhyUeD2D direct multicast air frame
  -> receiving LteUe PDCP / RLC / UDP
  -> MetsrBsmPc5App receive report
  -> METS-R bridge received_bsms/link_metrics
```

All `car[*]` UEs join `224.0.0.10` on the `cellular` interface through the
`<multicast-group>` entry in `sim5g/pc5.xml`. The PC5 app also calls
`joinLocalMulticastGroups` so INET delivers the datagram to its UDP socket.
The XML declaration is required because an INI-only multicast-group setting
did not populate Simu5G's Binder for this topology.

Multicast uses fixed D2D CQI because Simu5G's one-to-many D2D implementation
does not provide per-receiver CQI or HARQ feedback. Concurrent transmitters can
therefore cause receiver-specific capture or channel losses. Missing receive
callbacks are reported as `simu5g_pc5_receive_timeout`; the bridge does not
invent delivery.

The PC5 network uses `LteUe`, not `NrUe`. In Simu5G 1.4.4, `NrUe` registers
both LTE and NR identities in the multicast group. The duplicate RRC callback
tries to create the same RLC-UM RX `MacCid` and aborts.
Pure `LteUe` has one identity and follows Simu5G's exercised D2D multicast
path.

For PC5 timing, `application_tx_time_s` preserves the caller or Scenic clock.
`bridge_tx_time_s`, `tx_time_s`, `rx_time_s`, and `latency_ms` use the OMNeT++
radio-injection clock, keeping radio latency self-consistent across different
time origins.

### Simu5G wire payload handling

When `wire_payload.data_b64` is present, the bridge passes it as
`wire_payload_b64`. Both `MetsrBsmPc5App` and `MetsrBsmUuApp` validate the
base64 text and insert the actual octets into an INET `BytesChunk`. A receive
report includes:

- `wire_payload_present`
- `wire_payload_encoding=base64`
- `wire_payload_b64`
- `payload_bytes`

This permits byte-for-byte UPER transport verification. Without a wire
payload, the app uses a clearly marked `ByteCountChunk` compatibility fallback
that models size but does not claim to be encoded J2735.

### PC5 fidelity boundary

This backend is Simu5G's runnable network-controlled LTE D2D path. Its resource
control is analogous to LTE C-V2X Mode 3: the eNB schedules resources for
direct UE-to-UE transmission. It models Simu5G interference and channel
behavior plus its LTE PDCP/RLC/MAC/PHY stack.

It is not a standards-complete vehicular PC5 implementation and does not claim
a complete PSCCH/PSSCH/SCI procedure. It is also not autonomous LTE C-V2X
Mode 4 or NR-V2X Mode 2: Simu5G 1.4.4 does not implement their sensing-based
semi-persistent scheduling and resource-selection pipeline. Do not describe
results from this backend as Mode 4 or Mode 2 SPS results. The application
carrier is UDP/IP over the D2D bearer; it does not add WSMP/IEEE 1609.3.

### PC5 integration contract

PC5 reports `backendImplementation=simu5g_lte_d2d_pc5` and data path
`simu5g_lte_d2d_pc5_multicast`. The dispatcher must:

1. update each `car[index].mobility`;
2. create one `KIND_SIMU5G_BSM_REQUEST` per physical BSM, not per receiver;
3. pass a globally unique `message_id`, `payload_bytes`, and optional wire
   payload;
4. register the intended receiver-index set;
5. consume reports by `(message_id, receiver_index)` and ignore the sender;
6. verify returned wire bytes before declaring delivery; and
7. time out only missing receivers while preserving successful receivers from
   the same multicast transmission.

Semantic attacks should stay in Python as BSM transformations. Radio-resource
attacks should inject real load so the selected backend decides the resulting
latency and loss.
