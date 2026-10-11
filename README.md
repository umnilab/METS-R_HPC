This is the repository for the HPC module of [METS-R SIM](https://github.com/umnilab/METS-R_SIM). Docker is required, and it is highly recommended to start with the tutorials in the [`tutorials/`](tutorials/) folder. For the latest instructions, please refer to the online [document](https://umnilab.github.io/METS-R_doc).

## Setup

1. Install Python 3.10 or newer, then install the Python dependencies from the repository root:

   ```bash
   pip install -r requirements.txt
   ```

2. Install Docker:

   - Windows/macOS: install [Docker Desktop](https://docs.docker.com/get-started/introduction/get-docker-desktop/).
   - Linux: install Docker Engine and Docker Compose using the Docker instructions for your distribution.

3. Enable localhost access for Docker host networking. METS-R launches the simulator container with Docker host networking (`--net=host`) so the Python clients can connect to simulator ports on `localhost`.

   - Docker Desktop: open **Settings > Resources > Network**, turn on **Enable host networking**, then apply and restart Docker Desktop.
   - Linux Docker Engine: host networking is available by default.

4. Confirm that Docker and Docker Compose are available:

   ```bash
   docker --version
   docker-compose --version
   ```

## Client APIs

Query and control methods are grouped by topic in
[`models/query_APIs/`](models/query_APIs/) and
[`models/control_APIs/`](models/control_APIs/), such as
`query_public_vehicles.py` and `control_taxi.py`. Continue calling them on
`METSRClient` instances, for example `client.query_taxi()` or
`client.reposition_taxi(4, zoneID=3)`.

List every public client API, including compatibility aliases, without starting
or connecting to a simulator:

```python
from clients.METSRClient import METSRClient

for api in METSRClient.list_apis():
    print(f"{api['name']}{api['signature']}: {api['description']}")

taxi_controls = METSRClient.list_apis(category="control", topic="taxi")
```

Each entry includes `name`, `signature`, `description`, `category`, `topic`, and
the defining `module`. Categories are `query`, `control`, and `client`; topics
match the topic filenames without the `query_` or `control_` prefix. Client
methods such as `start_viz()` and `render()` remain available. Their live stream
startup, frame queries, browser delivery, and shutdown are implemented in
[`utils/visualization/client.py`](utils/visualization/client.py), inherited by
`METSRClient` without forwarding wrappers.

[`clients/METSRClient.py`](clients/METSRClient.py) implements client state and
workflows directly: configuration tracking and reset/restart decisions, connection
and readiness, request/response retries, RPC profiling, API discovery, and resource
cleanup. Read this file to follow a request, inspect client configuration or
counters, or understand a restart.

Keep implementations to three structural layers: **class → API method →
implementation with shared helpers**. Put the workflow directly in the method;
extract helpers for repeated operations, and avoid chains of single-use private
methods. For example, `send_msg()` validates and sends its request,
`send_receive_msg()` handles retries, and `reset()` contains both the reset and
restart paths. Shared connection, handshake, profiling, and encoding operations
remain helpers. The visualization queries and browser wait used by both rendering
and dashboards are public methods: `query_visualization_vehicles()` and
`wait_for_visualization_client()`.

New simulator commands belong in the query/control topic modules. Core connection
and request logic belongs in `METSRClient.py`; visualization state and behavior
belong in `utils/visualization/`. The client explicitly initializes visualization,
restores it after a restart, and stops it on close. Stateless configuration and
path helpers, log readers, and launch utilities belong in the modules below.

[`utils/util.py`](utils/util.py) exposes the implementation functions directly
for notebooks and experiment scripts: read configuration, prepare directories,
launch runs, and start/stop visualization. It adds no forwarding wrappers.
Only its Kafka workflow and visualization URL builder are implemented there.
The imported functions have these homes:

| Module | Responsibility |
| --- | --- |
| `utils/primitives.py` | Small value conversions, payload helpers, and socket probes. |
| `utils/configuration.py` | Stateless configuration loading, path normalization, option lookup, and property reading. |
| `utils/simulation_setup.py` | Per-instance properties, data directories, and thin-run preparation. |
| `utils/simulation_commands.py` | Java classpaths, JVM/JFR options, and container resource arguments. |
| `utils/simulation_handles.py` | Launch ownership, timing records, and scoped cleanup. |
| `utils/simulation_launch.py` | Local Java, Docker, and Apptainer launch workflows. |
| `utils/simulation_diagnostics.py` | Read fatal JVM log excerpts and startup phase timings; the client owns caching and polling. |
| `utils/visualization/` | Client visualization methods, binary protocol encoding, file servers, trajectory discovery, and the shared cleanup registry. |

Implementation code imports these modules directly. Helper-only exports from
`clients.METSRClient` have been removed; import helpers from their owning utility
modules. Private helpers formerly imported from `utils.util` also belong here, such as
`utils.simulation_setup._property_override_value` and
`utils.visualization.protocol._viz_chunk`.

The Scenic experiment runtime has its own [module guide](scenic_exp/scenarios/README.md#records-and-implementation-notes).
Run the focused client and runtime checks without simulator servers:

```bash
python -m unittest discover -s clients/tests
python -m unittest discover -s utils/tests
python -m unittest discover -s scenic_exp/tests
```

## Tutorials

The tutorial notebooks and runnable examples are in [`tutorials/`](tutorials/). Run commands from the repository root so relative paths such as `configs/...`, `data/...`, and `docker/...` resolve correctly. The Python tutorial scripts also switch to the repository root automatically when launched from another folder.

Recommended starting point:

```bash
jupyter lab tutorials/basic_tutorial.ipynb
```

If JupyterLab is not installed:

```bash
pip install jupyterlab
jupyter lab tutorials/basic_tutorial.ipynb
```

Additional notebooks:

- [`tutorials/advanced_commands.ipynb`](tutorials/advanced_commands.ipynb): command patterns and longer workflows.
- [`tutorials/security_examples.ipynb`](tutorials/security_examples.ipynb): security and attack-scenario examples.

### Security research replication demos

The dependency-free, simulator-safe five-demo suite is documented in [`tutorials/security_demos/`](tutorials/security_demos/). For example:

```bash
python tutorials/security_demos/demo_01_location_spoofing.py
```

Runnable tutorial scripts:

```bash
# CARLA/METS-R co-simulation example
python tutorials/cosim_example.py -r configs/run_cosim_CARLAT5.json -v

# Town05 BSM example; start one OMNeT++ radio backend first
python tutorials/v2x_veins_example.py -r configs/run_v2x_veins_Template.json
```

### PCLA dashboard debugging

Run the PCLA dashboard demos from the PCLA Conda environment and point
`PCLA_HOME` at the PCLA checkout (the directory containing `PCLA.py`):

```bash
conda activate PCLA
export PCLA_HOME=/path/to/PCLA
python -c 'import sys, PCLA; print(sys.executable); print(PCLA.__file__)'
```

If Scenic reports `ModuleNotFoundError: No module named 'PCLA'`, the active
Python process cannot see the PCLA checkout. Check `which python`, verify that
`$PCLA_HOME/PCLA.py` exists, and set `PCLA_HOME` in the same shell used to
launch the demo.

The default PCLA scenario generates an ego route under
`scenic_exp/helpers/routes`. Create that output directory if route generation
fails with `No such file or directory: .../ego_route.xml`:

```bash
mkdir -p scenic_exp/helpers/routes
```

SimLingo weights are not stored in the PCLA Git checkout. Current PCLA
versions download just that agent with:

```bash
cd /path/to/PCLA
python pcla_functions/download_weights.py --agents simlingo
```

If the downloader instead requests `pretrained.zip` and receives HTTP 404, it
is the retired single-archive downloader. Check for local PCLA changes before
updating it, then use the current resumable per-agent downloader:

```bash
cd /path/to/PCLA
git status --short
git pull --ff-only origin main
python -c 'import huggingface_hub; print(huggingface_hub.__version__)'
python pcla_functions/download_weights.py --agents simlingo
```

Install `huggingface_hub` into the active PCLA environment if the import check
fails. After downloading, verify
`pcla_agents/simlingo_pretrained/.hydra/config.yaml` and
`pcla_agents/simlingo_pretrained/checkpoints/epoch=013.ckpt/pytorch_model.pt`.

PCLA pins `antlr4-python3-runtime==4.9.3` for its OmegaConf configuration
parser. The error `Could not deserialize ATN with version 3 (expected 4)` means
that the active environment contains an incompatible ANTLR runtime. Inspect
and repair the package using the same Python executable that launches the
demo:

```bash
python -m pip show antlr4-python3-runtime omegaconf
python -m pip install --no-cache-dir --force-reinstall antlr4-python3-runtime==4.9.3
python -m pip show antlr4-python3-runtime omegaconf
```

The final command should report ANTLR `4.9.3` and OmegaConf `2.3.0`. A CARLA
warning that a sensor object went out of scope after one of these failures is
secondary cleanup noise: the exception interrupted the simulation after its
sensors were spawned. Fix the preceding exception and reload or restart the
CARLA world to remove any leftover sensor actor.

## New: CARLA Visualization Integration

This repository now includes enhanced CARLA visualization functionality that allows you to:

- **Start a dedicated CARLA instance** for METS-R simulation visualization
- **Display METS-R vehicles in real-time** within the CARLA environment
- **Automatically synchronize** vehicle positions between METS-R simulation and CARLA
- **Manage vehicle lifecycle** (spawning, updating, cleanup) automatically

### Quick Start with CARLA Visualization

```bash
# Start simulation with CARLA visualization
python tutorials/cosim_example.py -r configs/run_cosim_CARLAT5.json -v

# Use a different CARLA co-simulation config
python tutorials/cosim_example.py -r configs/run_cosim_CARLAT1.json -v
```

CARLA settings such as `carla_dir`, `carla_host`, `carla_port`, and `carla_map` are defined in the selected run config under [`configs/`](configs/). `onConnector` becomes false when a vehicle leaves the connector.
Native vehicles near downstream road entries are exposed by
`query_boundary_vehicle(boundary_dist=6.0)` (wire message
`boundaryVehicle`, simulator handler `queryBoundaryVeh`). The updated handler
requires a top-level `boundaryDist` in meters, which the client always sends:

```python
controlled = metsr.query_cosim_vehicle()["data"]
boundary = metsr.query_boundary_vehicle(boundary_dist=6.0)["data"]
if boundary:
    boundary_poses = metsr.query_vehicle(
        id=[vehicle["vehicleId"] for vehicle in boundary],
        private_veh=[vehicle["isPrivate"] for vehicle in boundary],
        transform_coords=True,
    )["data"]
```

Boundary records use the co-simulation vehicle schema but retain native
road/lane IDs and `controlMode="native"`. METS-R selects vehicles strictly less
than `boundary_dist` meters from their current lane's entry, independent of vehicle
length. The distance must be finite and non-negative; zero selects no vehicles.
The client supplies a **6 m default** for existing no-argument calls.`step_carla_metsr_cosim`
automatically mirrors these blockers before each CARLA tick, even with
`display_all=False`, and removes them when they leave the boundary. Their
CARLA actors have autopilot and physics disabled and follow METS-R poses.
Only externally owned vehicles send poses through `teleport_cosim_vehicle`.

## Packet-level V2X Backends

`omnetpp_bridge/` contains separate implementations as several radios:

- `Veins80211p` dynamically creates real Veins 5.3.1
  `Nic80211p`/`Mac1609_4`/`PhyLayer80211p` vehicles driven by METS-R mobility.
- `Sim5gCv2xPc5` uses Simu5G 1.4.4's runnable LTE D2D multicast stack for a
  direct, eNodeB-scheduled transmission and reports each UE receive event.
- `Sim5gCellularUu` as the infrastructure-mediated NR Uu path.
- `AbstractOmnetpp` is available for fast analytical tests.

Build and start exactly one radio backend from WSL:

```bash
cd ~/src/METS-R_HPC/omnetpp_bridge

# Real Veins 802.11p
bash ./build_veins.sh
bash ./run_veins_80211p.sh

# Or network-controlled Simu5G LTE D2D/PC5
bash ./build_sim5g.sh
bash ./run_sim5g_pc5.sh
```

Then run the broadcast BSM example:

```bash
python tutorials/v2x_veins_example.py \
  -r configs/run_v2x_veins_Template.json \
  --vehicle_source town05_seed --ticks 5
```

The default data format remains a json explicitly labelled `SAE J2735-aligned`.
For real ASN.1/UPER bytes, install `requirements-j2735.txt`, supply your
revision-matched SAE ASN.1 modules, and select strict
`veins_j2735_codec=uper`. See [J2735 UPER setup](docs/j2735_uper.md) and the
[radio backend guide](omnetpp_bridge/README.md) for requirements, commands, and
the exact fidelity boundaries.

## License

Copyright (c) 2026 UMNILAB.

This repository is licensed by UMNILAB under the Creative Commons Attribution 4.0 International License. See [LICENSE](LICENSE).
