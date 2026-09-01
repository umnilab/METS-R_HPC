#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
: "${VEINS_HOME:?Set VEINS_HOME to a built Veins 5.3.1 checkout}"
GENERATED_NED_ROOT="${VEINS_GENERATED_NED_ROOT:-.generated/veins-ned}"

test -f "$GENERATED_NED_ROOT/MetsrVeins80211pNetwork.ned" || {
    echo "Generated Veins NED files not found; run build_veins.sh first." >&2
    exit 1
}

library="$(find . -type f -name 'libmetsr_veins_bridge_80211p*.so' | head -n 1)"
test -n "$library" || {
    echo "Veins bridge library not found; run build_veins.sh first." >&2
    exit 1
}
library="${library#./}"
library_dir="$(dirname "$library")"
library_base="$(basename "$library")"
library="$library_dir/${library_base#lib}"
library="${library%.so}"

opp_run \
    -u Cmdenv \
    -n "$GENERATED_NED_ROOT:$VEINS_HOME/src/veins" \
    -l "$VEINS_HOME/src/veins" \
    -l "$library" \
    -c Veins80211p \
    omnetpp.ini
