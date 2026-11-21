#!/bin/sh
# Time the submitted image against a candidate binary on a dataset in a Docker volume, alternating runs,
# with the scorer's docker flags (memory MEM, default 12g). Usage:
#   sh tests/bench_native.sh VOLUME RUNS [BASE_IMAGE]
# The candidate is /target/release/shopping_sessions in the native-target volume (cargo build --release).
# Prints one line per run: which, pipeline total (from its stderr) and docker wall time.
set -eu
VOL="$1"; RUNS="$2"; BASE="${3:-shopping-sessions:latest}"; MEM="${MEM:-12g}"
export MSYS_NO_PATHCONV=1
docker volume create bench_out >/dev/null
docker volume create bench_scratch >/dev/null
run() {
  docker run --rm --user 0 -v bench_out:/o -v bench_scratch:/s --entrypoint sh "$BASE" -c 'rm -rf /o/* /s/*; chmod 777 /o /s'
  flags="--rm --platform linux/amd64 --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 --cpus 8 --memory $MEM --memory-swap $MEM \
    --user 1000:1000 -v $VOL:/data:ro -v bench_out:/output -v bench_scratch:/scratch \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU=8 -e MEM_GB=${MEM%g}"
  s=$(date +%s%N)
  if [ "$1" = base ]; then
    log=$(docker run $flags "$BASE" 2>&1 >/dev/null)
  else
    log=$(docker run $flags -v native-target:/target:ro --entrypoint /target/release/shopping_sessions "$BASE" 2>&1 >/dev/null)
  fi
  e=$(date +%s%N)
  echo "$1 $(echo "$log" | grep total | tr -s ' ') wall $(( (e - s) / 1000000 )) ms"
}
docker run --rm --user 0 -v bench_out:/o -v bench_scratch:/s --entrypoint sh "$BASE" -c 'chmod 777 /o /s'
i=0
while [ $i -lt "$RUNS" ]; do
  run base
  run cand
  i=$((i + 1))
done
