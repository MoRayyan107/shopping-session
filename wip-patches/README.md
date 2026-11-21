# Rescued unverified work from the D: repo (2026-11-21)

NOT verified against the oracle. Do not merge to main as-is.

- `01-prefetch-working-tree.patch`: the uncommitted working tree of branch `speed` on top of 43d68e3
  (raw::prefetch with madvise WILLNEED / pread, util::map_in_order, NOTES speed section, scorer_run.sh
  COLD/BIN/TIMEOUT_S changes, new tests/bench_native.sh). Cold p20m pipeline 2.85-2.92 s vs 3.36-3.52 s (unverified).
  Apply: `git apply wip-patches/01-prefetch-working-tree.patch`
- `02-stash-8byte-splitter.patch`: `stash@{0}` "8-byte splitter (unverified)", made on 597cf7d; native/src/csv.rs only.
  Applies cleanly to dcc09e9 and on top of 01. Not timed or verified.

Both were checked byte for byte against the D: files they came from.
