# Simu5G Cellular Uu Backend Scaffold

This folder contains the Simu5G cellular Uu backend pieces for the existing
METS-R/CARLA -> Python -> JSON -> OMNeT++ protocol.

The current `Sim5gCellular` config in `../omnetpp.ini` is still an abstract
event profile. Use `Sim5gCellularUu` when you want the real Simu5G/INET path:
METS-R vehicle positions update Simu5G NR UE mobility, and BSMs are sent by UE
applications over UDP through the Simu5G NR Uu stack.

## External Stack

Install and build OMNeT++, INET, and Simu5G first. The Simu5G installation docs
recommend `opp_env`, but `opp_env` is a separate helper tool. If your shell says
`opp_env: command not found`, skip this command and use the manual install path
below.

```bash
opp_env install simu5g-latest
```

For a manual install, download/build INET and Simu5G next to your OMNeT++
checkout. The current Simu5G docs describe command-line builds with:

```bash
source "$OMNETPP_HOME/setenv"
cd "$INET_HOME"
. setenv
make makefiles
make MODE=release

cd "$SIMU5G_HOME"
. setenv
make makefiles
make MODE=release
```

Then point this repo at those builds:

```bash
export OMNETPP_HOME=/path/to/omnetpp
export INET_HOME=/path/to/inet4.5
export SIMU5G_HOME=/path/to/Simu5G
source "$OMNETPP_HOME/setenv"
cd "$SIMU5G_HOME"
. setenv

cd /path/to/METS-R_HPC/veins_bridge/omnetpp
bash ./check_sim5g_env.sh
bash ./build_sim5g.sh
bash ./run_sim5g_uu.sh
```

## Integration Contract

The first real implementation should keep the current Python client contract:

- Python sends `sync_tick` with `vehicles`, `bsm_messages`, and optional
  `attacks`.
- OMNeT++ maps each METS-R vehicle record to a Simu5G UE mobility state at the
  tick boundary.
- OMNeT++ injects each BSM payload into a UE application.
- Simu5G/INET decides delivery, latency, and loss through the configured NR Uu
  topology.
- The bridge returns `received_bsms`, `link_metrics`, and `attack_events`.

`link_metrics` should add Simu5G-specific fields when available:

- `serving_gnb`
- `cqi`
- `sinr_db`
- `rsrp_dbm`
- `bler`
- `harq_retx`
- `scheduler_delay_ms`
- `handover_state`
- `bearer_qci` or `five_qi`

## Generated Files

The `.template` files are materialized by `../build_sim5g.sh` into
`../../.generated/sim5g-ned`. Keeping the live `.ned` files outside the default
OMNeT++ source directory prevents the abstract `opp_run -n .` workflow from
loading Simu5G imports when you are not running the Simu5G backend.

## Connected Vehicle Interpretation

`Sim5gCellularUu` models infrastructure-mediated connected-vehicle traffic:

- V2V over Uu: a vehicle UE sends a BSM to another vehicle UE through the
  cellular network path.
- V2N/V2I over Uu: a vehicle UE sends status, BSM, or query-response payloads to
  a network, MEC, traffic-management, or infrastructure endpoint.
- V2X over Uu: the same user-plane path represents any cellular-reachable target.

This is not PC5 sidelink. PC5 should be implemented as a separate backend for
direct V2V/RSU/VRU broadcast or multicast with sidelink resource selection and
interference/collision behavior.
