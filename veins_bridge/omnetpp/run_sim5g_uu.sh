#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

bash ./check_sim5g_env.sh

GENERATED_NED_ROOT="${GENERATED_NED_ROOT:-../.generated/sim5g-ned}"
GENERATED_INI="${GENERATED_INI:-$GENERATED_NED_ROOT/omnetpp-sim5g-uu.ini}"

if [ ! -f "$GENERATED_NED_ROOT/Sim5gCellularUuBridgeNetwork.ned" ]; then
    echo "Generated NED files are missing. Run bash ./build_sim5g.sh first." >&2
    exit 1
fi
if [ ! -f "$GENERATED_NED_ROOT/sim5g/demo.xml" ]; then
    mkdir -p "$GENERATED_NED_ROOT/sim5g"
    cp sim5g/demo.xml "$GENERATED_NED_ROOT/sim5g/demo.xml"
    echo "Copied missing Simu5G routing XML to $GENERATED_NED_ROOT/sim5g/demo.xml"
fi

LIB_SO="$(find out -name 'libmetsr_veins_bridge_simu5g.so' | head -n 1)"
if [ -z "$LIB_SO" ]; then
    echo "Could not find libmetsr_veins_bridge_simu5g.so. Run bash ./build_sim5g.sh first." >&2
    exit 1
fi
LIB_STEM="$(dirname "$LIB_SO")/$(basename "$LIB_SO" .so)"
LIB_STEM="${LIB_STEM%/libmetsr_veins_bridge_simu5g}/metsr_veins_bridge_simu5g"

NED_PATH=".:$GENERATED_NED_ROOT:$SIMU5G_HOME/src:$SIMU5G_HOME/simulations:$INET_HOME/src:$INET_HOME/examples"

opp_run \
    -u Cmdenv \
    -n "$NED_PATH" \
    -l "$INET_HOME/src/INET" \
    -l "$SIMU5G_HOME/src/simu5g" \
    -l "$LIB_STEM" \
    -c General \
    "$GENERATED_INI"
