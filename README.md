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

Runnable tutorial scripts:

```bash
# CARLA/METS-R co-simulation example
python tutorials/cosim_example.py -r configs/run_cosim_CARLAT5.json -v

# Veins latency stress example; start the OMNeT++/Veins bridge first
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json
```

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

## Experimental: OMNeT++/VEINS V2X Client

The Python clients include an experimental bridge client for packet-level V2X
experiments. A real OMNeT++/VEINS bridge must already be listening on the
configured host/port. The remaining tutorial sends a synthetic noise-message
load toward one target vehicle and reports the latency values returned by the
Veins bridge.

Build and start the included OMNeT++ bridge from WSL:

```bash
export OMNETPP_HOME=~/src/omnetpp-6.1
source "$OMNETPP_HOME/setenv"

cd ~/src/METS-R_HPC/veins_bridge/omnetpp
bash ./build.sh
opp_run -u Cmdenv -n . -l ./out/gcc-release/src/libmetsr_veins_bridge omnetpp.ini
```

Then run the Python latency example from this repository:

```bash
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json \
  --noise_senders 60 --messages_per_sender 10 --ticks 100 --csv output/veins_latency.csv
```

Veins installation and protocol notes have moved to the online
[METS-R documentation](https://umnilab.github.io/METS-R_doc).

## License

Copyright (c) 2026 UMNILAB.

This repository is licensed by UMNILAB under the Creative Commons Attribution 4.0 International License. See [LICENSE](LICENSE).
