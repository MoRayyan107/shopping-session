#!/bin/sh
# Alternating A/B timing with the scorer's flags (tests/scorer_run.sh): the base image's own binary against the
# candidate in the native-target volume, cold (page cache dropped, drop_caches=1) and warm.
#   sh tests/ab.sh VOLUME RUNS [BASE_IMAGE] [MEM]
# CAND_VOL names the volume holding the candidate (default native-target).
# One line per run: which, cold/warm, exit, wall ms (whole docker run), pipeline total.
set -eu
VOL="$1"; RUNS="$2"; BASE="${3:-shopping-sessions:latest}"; MEM="${4:-12g}"
T="$(dirname "$0")/scorer_run.sh"
line() { echo "$1 $2 $(echo "$3" | grep -o 'exit=[^ ]* wall_ms=[0-9]* pipeline=[^ ]*')"; }
i=0
while [ $i -lt "$RUNS" ]; do
  for mode in cold warm; do
    c=0; [ $mode = cold ] && c=1
    line base $mode "$(COLD=$c sh "$T" "$BASE" "$VOL" ab_base "$MEM")"
    line cand $mode "$(COLD=$c BIN=/target/release/shopping_sessions EXTRA_MOUNT="-v ${CAND_VOL:-native-target}:/target:ro" \
      sh "$T" "$BASE" "$VOL" ab_cand "$MEM")"
  done
  i=$((i + 1))
done
