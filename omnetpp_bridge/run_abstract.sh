#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

command -v opp_run >/dev/null 2>&1 || {
    echo "opp_run was not found. Source setenv from your OMNeT++ install first." >&2
    exit 1
}

GENERATED_NED_ROOT="${ABSTRACT_GENERATED_NED_ROOT:-.generated/abstract-ned}"
for ned_name in MetsrVeinsBridge MetsrVeinsBridgeNetwork; do
    test -f "$GENERATED_NED_ROOT/$ned_name.ned" || {
        echo "Generated abstract NED files not found; run build.sh first." >&2
        exit 1
    }
done

library="$(find . -type f -name 'libmetsr_veins_bridge.so' | head -n 1)"
test -n "$library" || {
    echo "Abstract bridge library not found; run build.sh first." >&2
    exit 1
}
library="${library#./}"
library_dir="$(dirname "$library")"
library_base="$(basename "$library")"
library="$library_dir/${library_base#lib}"
library="${library%.so}"

opp_run \
    -u Cmdenv \
    -n "$GENERATED_NED_ROOT" \
    -l "$library" \
    -c AbstractOmnetpp \
    omnetpp.ini
