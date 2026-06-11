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

if [ "$missing" -ne 0 ]; then
    echo
    echo "Simu5G environment is incomplete."
    echo "Recommended install path from Simu5G docs:"
    echo "  opp_env install simu5g-latest"
    echo
    echo "Or set these manually after building OMNeT++, INET, and Simu5G:"
    echo "  export OMNETPP_HOME=/path/to/omnetpp"
    echo "  export INET_HOME=/path/to/inet4.5"
    echo "  export SIMU5G_HOME=/path/to/Simu5G"
    echo "  source \"\$OMNETPP_HOME/setenv\""
    echo "  cd \"\$SIMU5G_HOME\" && . setenv"
    exit 1
fi

echo
echo "Suggested NED path for the future Simu5G Uu bridge:"
echo "  .:../.generated/sim5g-ned:\$SIMU5G_HOME/src:\$SIMU5G_HOME/simulations:\$INET_HOME/src:\$INET_HOME/examples"
echo
if [ -d "$SIMU5G_HOME/simulations/nr" ]; then
    echo "found Simu5G NR examples: $SIMU5G_HOME/simulations/nr"
elif [ -d "$SIMU5G_HOME/simulations/NR" ]; then
    echo "found Simu5G NR examples: $SIMU5G_HOME/simulations/NR"
else
    echo "warning: could not find simulations/nr or simulations/NR under SIMU5G_HOME" >&2
fi

echo
echo "Environment check passed. Run bash ./build_sim5g.sh to generate NED files"
echo "and compile the METS-R bridge with the Simu5G UE app/mobility modules."
