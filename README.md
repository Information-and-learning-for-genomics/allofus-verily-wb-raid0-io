# storage→GPU RAID0 raw-IO bench on Verily Workbench Dataproc (T4 in path)

Reproduce the per-box **6.1–6.7 GiB/s** 16-local-SSD RAID0 raw throughput with a T4 GPU in
the data path (direct-IO storage reads feeding a pinned host→GPU copy). All numbers are
reported as **RAW GiB/s to the compute stream only** — encoding-based "effective speed"
arguments are out of scope and banned as perf claims.

**Status: bench numbers pending — this repo documents the certified procedure.**

Canonical paste source: [`READABLE.md`](READABLE.md) (rendered, copy buttons work; GitHub's
notebook viewer truncates long cells). Runnable artifact:
[`local-ssd-raid0-io-test.ipynb`](local-ssd-raid0-io-test.ipynb) (generated from READABLE.md
by `python3 make_ipynb.py` — never hand-edit it).

## 1. Order a cluster (`wb` = Verily Workbench CLI, on PATH)

```
wb resource create dataproc-cluster --id=<ID> --region=us-central1 --quiet --format=JSON \
  --num-workers=2 --worker-machine-type=n1-standard-32 \
  --worker-boot-disk-size=100 --worker-num-local-ssds=16 \
  --worker-accelerator-type=nvidia-tesla-t4 --worker-accelerator-count=1
```

`wb resource create` / `delete` block silently for 10–20 min (server-side flight) — run
them where blocking costs nothing, and check with the instant `wb resource list/describe`.

## 2. Open JupyterLab

```
wb resource describe --id=<ID> --format=JSON    # -> proxyUri = the JupyterLab URL
```

Upload/open `local-ssd-raid0-io-test.ipynb` with the Python 3 kernel; keep the kernel alive
during a run. If you prefer copying text, copy cell-by-cell from READABLE.md in C0→C8 order.

## 3. Execution model (binding — the whole point of this layout)

1. **Fresh kernel (or brand-new cluster) → run the file TOP-DOWN, ONCE: C0 → C1 → … → C8.**
2. **Individual cells are NEVER re-run.** There is no "resume at cell X" state.
3. The ONLY recovery from any failure, hang, weird marker, or kernel restart: **restart the
   kernel and run the file again from C0.** Every cell is idempotent (stripe skips if
   mounted, fill skips if the file size is exact, a per-run random tag makes each worker
   task fresh), so a top-down replay always lands in exactly the state a first-time runner
   sees.
4. A kernel restart does **NOT** clean the workers — executors, YARN apps, containers and
   bench processes survive it. That is why C0 (kill YARN) is always first.
5. Any cell printing `PREFLIGHT FAIL`, or `MODE: spark-submit fallback` after you ran C2,
   means the contract was broken → restart kernel, run from C0.

Each cell declares its GREEN marker (table at the top of READABLE.md, repeated above every
notebook cell); markers must arrive from BOTH workers.

## 4. Cost honesty

- Ordered shape 2× [n1-standard-32 + 16×375 GiB local SSDs + 1× T4]: **$3.83/h**
  (console-metered, 2026-10-02, cluster t4-io5, both workers + master included).
  Note on pricing mechanics: GCP docs DO bill local SSD per provisioned GiB separately, so raw
  list with the SSD line is $5.09-5.82/h (derived) — the console's $3.83/h reflects contracted
  tenant pricing. Quote only console-metered values; raw-list arithmetic misleads here.
- Verified fact on this tenant: GPU Dataproc workers may be **silently templated** down to
  n1-standard-8 + 8 SSDs + T4 — **$1.07/h** (console-metered, cluster t4-io3) — a different
  machine than you ordered (8 SSDs cannot reach the 16-disk speed cap).
- **Truth source: the C4 probe lines `GCE_MT` and `SSD_DATA`.** Interpret every result
  against what C4 actually saw, never against what was ordered.
- Clusters meter while alive. Delete when done: `wb resource delete --id=<ID> --quiet`.
