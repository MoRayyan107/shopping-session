#!/bin/sh
# One scorer-style run of an image on a dataset held in a Docker volume, keeping everything for the record:
#   sh tests/scorer_run.sh IMAGE DATA_VOLUME NAME [MEM]        (MEM default 12g; env EXTRA_ENV="-e K=V ...")
# The container is kept (no --rm, named run_NAME). It writes to the volumes out_NAME / scr_NAME, and the
# outputs are copied to $HOSTDIR/NAME (default ../dockertmp/NAME) for scripts/compare.py.
# Inside, a sampler records anon/file memory (cgroup memory.stat) every 0.2 s; memory.peak is read at the end.
# Prints: exit code, wall ms, pipeline total, anon peak, file at that moment, memory.peak, major faults.
set -eu
IMAGE="$1"; VOL="$2"; NAME="$3"; MEM="${4:-12g}"
HOSTDIR="${HOSTDIR:-$(cd "$(dirname "$0")/../.." && pwd)/dockertmp}"
# abort on an empty value: these name the folders and volumes that get cleaned below
: "${IMAGE:?}" "${VOL:?}" "${NAME:?}" "${MEM:?}" "${HOSTDIR:?}"
case "$NAME" in */*|.|..) echo "bad NAME: $NAME" >&2; exit 2;; esac
export MSYS_NO_PATHCONV=1
docker rm -f "run_$NAME" >/dev/null 2>&1 || true
for v in "out_$NAME" "scr_$NAME"; do docker volume create "$v" >/dev/null; done
docker run --rm --user 0 -v "out_$NAME":/o -v "scr_$NAME":/s --entrypoint sh "$IMAGE" \
  -c 'rm -rf /o/* /s/*; chmod 777 /o /s'
s=$(date +%s%N)
# TIMEOUT_S: stop the container after that many seconds (the run is then recorded as exit 137)
killer=""; if [ -n "${TIMEOUT_S:-}" ]; then (sleep "$TIMEOUT_S"; docker stop -t 1 "run_$NAME" >/dev/null 2>&1) & killer=$!; fi
docker run --name "run_$NAME" --platform linux/amd64 \
  --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 4096 --cpus 8 --memory "$MEM" --memory-swap "$MEM" \
  --user 1000:1000 \
  -v "$VOL":/data:ro -v "out_$NAME":/output -v "scr_$NAME":/scratch \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
  -e NCPU=8 -e MEM_GB="${MEM%g}" ${EXTRA_ENV:-} \
  --entrypoint sh "$IMAGE" -c '
    ( while :; do awk "/^(anon|file|pgmajfault) /{printf \"%s \", \$2} END{print \"\"}" /sys/fs/cgroup/memory.stat; sleep 0.2; done ) > /scratch/mem.log &
    sampler=$!
    /app/shopping_sessions 2> /scratch/log.txt; code=$?
    kill $sampler
    echo $code > /scratch/exit
    cat /sys/fs/cgroup/memory.peak > /scratch/peak
    grep -E "^(oom|oom_kill) " /sys/fs/cgroup/memory.events > /scratch/events
    exit $code' || true
e=$(date +%s%N)
[ -n "$killer" ] && kill "$killer" 2>/dev/null || true
mkdir -p "${HOSTDIR:?}/${NAME:?}"
# clear the last run's record, so a missing file can never be mistaken for this run's
rm -rf "${HOSTDIR:?}/${NAME:?}/out"
rm -f "${HOSTDIR:?}/${NAME:?}/exit" "${HOSTDIR:?}/${NAME:?}/peak" "${HOSTDIR:?}/${NAME:?}/events" \
  "${HOSTDIR:?}/${NAME:?}/log.txt" "${HOSTDIR:?}/${NAME:?}/mem.log"
docker run --rm --user 0 -v "scr_$NAME":/s:ro -v "out_$NAME":/o:ro -v "$HOSTDIR/$NAME":/h --entrypoint sh "$IMAGE" \
  -c 'cp /s/log.txt /s/mem.log /s/exit /s/peak /s/events /h/ 2>/dev/null; mkdir -p /h/out; cp -r /o/. /h/out/'
code=$(cat "$HOSTDIR/$NAME/exit" 2>/dev/null || echo "none (container $(docker inspect -f '{{.State.ExitCode}} oom={{.State.OOMKilled}}' "run_$NAME"))")
total=$(grep -o '\[ *[0-9.]*s\] total' "$HOSTDIR/$NAME/log.txt" | tr -d '[]' | awk '{print $1}' || true)
peak=$(sort -n "$HOSTDIR/$NAME/mem.log" | tail -1)
echo "$NAME image=$IMAGE mem=$MEM exit=$code wall_ms=$(( (e - s) / 1000000 )) pipeline=${total:-?}" \
  "anon_peak_GB=$(echo "$peak" | awk '{printf "%.2f", $1/1e9}') file_then_GB=$(echo "$peak" | awk '{printf "%.2f", $2/1e9}')" \
  "memory.peak_GB=$(awk '{printf "%.2f", $1/1e9}' "$HOSTDIR/$NAME/peak" 2>/dev/null)" \
  "majfault=$(tail -1 "$HOSTDIR/$NAME/mem.log" | awk '{print $3}') $(cat "$HOSTDIR/$NAME/events" 2>/dev/null | tr '\n' ' ')"
