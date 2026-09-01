#!/usr/bin/env bash
set -euo pipefail

missing=0

require_command() {
    local name="$1"
    if ! command -v "$name" >/dev/null 2>&1; then
        echo "missing command: $name" >&2
        missing=1
    else
        echo "found command: $name ($(command -v "$name"))"
    fi
}

require_dir_var() {
    local var_name="$1"
    local value="${!var_name:-}"
    if [ -z "$value" ]; then
        echo "missing env var: $var_name" >&2
        missing=1
        return
    fi
    if [ ! -d "$value" ]; then
        echo "missing directory from $var_name: $value" >&2
        missing=1
        return
    fi
    echo "found $var_name=$value"
}

require_command opp_run
require_command opp_makemake
require_dir_var OMNETPP_HOME
require_dir_var INET_HOME
require_dir_var SIMU5G_HOME

if [ "$missing" -eq 0 ]; then
    REQUIRED_SIMU5G_VERSION="${METSR_SIMU5G_VERSION:-1.4.4}"
    SIMU5G_VERSION_FILE="$SIMU5G_HOME/Version"
    if [ ! -f "$SIMU5G_VERSION_FILE" ]; then
        echo "missing Simu5G version file: $SIMU5G_VERSION_FILE" >&2
        missing=1
    else
        ACTUAL_SIMU5G_VERSION="$(tr -d '[:space:]' < "$SIMU5G_VERSION_FILE")"
        if [ "$ACTUAL_SIMU5G_VERSION" != "$REQUIRED_SIMU5G_VERSION" ] && \
           [ "${METSR_ALLOW_UNTESTED_SIMU5G:-0}" != "1" ]; then
            echo "unsupported Simu5G version: $ACTUAL_SIMU5G_VERSION" >&2
            echo "this bridge targets Simu5G $REQUIRED_SIMU5G_VERSION exactly" >&2
            echo "set METSR_ALLOW_UNTESTED_SIMU5G=1 only after validating the NED/C++ API" >&2
            missing=1
        else
            echo "found Simu5G version: $ACTUAL_SIMU5G_VERSION"
        fi
    fi

    REQUIRED_SIMU5G_FILES=(
        "$SIMU5G_HOME/src/simu5g/stack/LteNicUeD2D.ned"
        "$SIMU5G_HOME/src/simu5g/stack/LteNicEnbD2D.ned"
        "$SIMU5G_HOME/src/simu5g/stack/mac/LteMacUeD2D.cc"
        "$SIMU5G_HOME/src/simu5g/stack/phy/LtePhyUeD2D.cc"
        "$SIMU5G_HOME/src/simu5g/nodes/PgwStandard.ned"
    )
    for required_file in "${REQUIRED_SIMU5G_FILES[@]}"; do
        if [ ! -f "$required_file" ]; then
            echo "missing required Simu5G PC5/D2D API file: $required_file" >&2
            missing=1
        fi
    done

    if [ "$missing" -eq 0 ]; then
        if ! grep -Fq 'd2dInitialMode' "$SIMU5G_HOME/src/simu5g/stack/LteNicUeD2D.ned"; then
            echo "Simu5G LteNicUeD2D does not expose d2dInitialMode" >&2
            missing=1
        fi
        if ! grep -Fq 'D2D_MULTI' "$SIMU5G_HOME/src/simu5g/stack/mac/LteMacUeD2D.cc" || \
           ! grep -Fq 'D2D_MULTI' "$SIMU5G_HOME/src/simu5g/stack/phy/LtePhyUeD2D.cc"; then
            echo "Simu5G LTE D2D MAC/PHY does not expose the required D2D_MULTI pipeline" >&2
            missing=1
        fi
        if ! grep -Fq 'inout filterGate' "$SIMU5G_HOME/src/simu5g/nodes/PgwStandard.ned"; then
            echo "Simu5G PgwStandard does not expose the v1.4.4 filterGate API" >&2
            missing=1
        fi
    fi
fi

if [ "$missing" -ne 0 ]; then
    echo
    echo "Simu5G environment is incomplete."
    echo "This bridge requires the explicit Simu5G v1.4.4 source/API:"
    echo "  git clone --branch v1.4.4 https://github.com/Unipisa/Simu5G.git Simu5G-1.4.4"
    echo "Do not substitute simu5g-latest without porting and validation."
    echo
    echo "If opp_env is not installed, build INET and Simu5G manually, then set:"
    echo "  export OMNETPP_HOME=/path/to/omnetpp"
    echo "  export INET_HOME=/path/to/inet4.5"
    echo "  export SIMU5G_HOME=/path/to/Simu5G-1.4.4"
    echo "  source \"\$OMNETPP_HOME/setenv\""
    echo "  cd \"\$INET_HOME\" && make makefiles && make MODE=release"
    echo "  cd \"\$SIMU5G_HOME\" && . setenv"
    echo "  make makefiles && make MODE=release"
    exit 1
fi

echo
echo "Suggested NED path for the Simu5G Uu and PC5 bridges:"
echo "  .generated/sim5g-ned:\$SIMU5G_HOME/src:\$SIMU5G_HOME/simulations:\$INET_HOME/src:\$INET_HOME/examples"
echo
if [ -d "$SIMU5G_HOME/simulations/lte/d2d_multicast" ]; then
    echo "found Simu5G LTE D2D multicast example: $SIMU5G_HOME/simulations/lte/d2d_multicast"
elif [ -d "$SIMU5G_HOME/simulations/LTE/D2D" ]; then
    echo "found Simu5G LTE D2D examples: $SIMU5G_HOME/simulations/LTE/D2D"
elif [ -d "$SIMU5G_HOME/simulations/lte/d2d" ]; then
    echo "found Simu5G LTE D2D examples: $SIMU5G_HOME/simulations/lte/d2d"
else
    echo "warning: could not find simulations/lte/d2d_multicast under SIMU5G_HOME" >&2
fi

echo
echo "Environment check passed for Simu5G 1.4.4 LTE D2D multicast."
echo "Run bash ./build_sim5g.sh to generate NED files and compile both the"
echo "legacy Uu app and the PC5/D2D multicast app."
