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

CARLA settings such as `carla_dir`, `carla_host`, `carla_port`, and `carla_map` are defined in the selected run config under [`configs/`](configs/).

The co-simulation bridge requires METS-R SIM commit
[`abdda1a`](https://github.com/umnilab/METS-R_SIM/commit/abdda1a5ee18c936cc152037b2d36bd388131612)
or newer. `onConnector` becomes false when a vehicle leaves the connector.
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
The client supplies a **6 m default** for existing no-argument calls. This replaces
the old server rule of 1.2 times each vehicle's length. `step_carla_metsr_cosim`
automatically mirrors these blockers before each CARLA tick, even with
`display_all=False`, and removes them when they leave the boundary. Their
CARLA actors have autopilot and physics disabled and follow METS-R poses.
Only externally owned vehicles send poses through `teleport_cosim_vehicle`.

## Packet-level V2X Backends

`omnetpp_bridge/` now contains separate implementations rather than one
OMNeT++ profile presented as several radios:

- `Veins80211p` dynamically creates real Veins 5.3.1
  `Nic80211p`/`Mac1609_4`/`PhyLayer80211p` vehicles driven by METS-R mobility.
- `Sim5gCv2xPc5` uses Simu5G 1.4.4's runnable LTE D2D multicast stack for a
  direct, eNodeB-scheduled transmission and reports each UE receive event.
- `Sim5gCellularUu` remains the infrastructure-mediated NR Uu path.
- `AbstractOmnetpp` remains available for fast analytical regressions.

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

The default payload mode remains explicitly labelled `SAE J2735-aligned`.
For real ASN.1/UPER bytes, install `requirements-j2735.txt`, supply your
revision-matched SAE ASN.1 modules, and select strict
`veins_j2735_codec=uper`. See [J2735 UPER setup](docs/j2735_uper.md) and the
[radio backend guide](omnetpp_bridge/README.md) for requirements, commands, and
the exact fidelity boundaries.

## License

Copyright (c) 2026 UMNILAB.

This repository is licensed by UMNILAB under the Creative Commons Attribution 4.0 International License. See [LICENSE](LICENSE).
