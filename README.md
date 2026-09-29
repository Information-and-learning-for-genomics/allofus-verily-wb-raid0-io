# A recipe to get the highest disk io throughput in Research All of Us Verily Workbench

> **Reading or copying this on GitHub? Open [READABLE.md](READABLE.md)** - identical content,
> rendered as normal documentation with proper copy buttons on every code block
> (GitHub's built-in notebook viewer truncates long cells and copies badly).
> [local-ssd-raid0-io-test.ipynb](local-ssd-raid0-io-test.ipynb) is the file you run.

What we order and what we do, step by step (all commands and code are in
[`local-ssd-raid0-io-test.ipynb`](local-ssd-raid0-io-test.ipynb), run top to bottom):

- a Dataproc cluster in the Workbench: 2 workers, `n2-standard-32` (32 CPUs, 128 GB RAM)
- with **16 local SSDs (375 GiB each) per worker** — the number of local SSDs is what sets
  the speed cap, so we order the most we can
- the workers' 16 disks are glued into one RAID0 volume (`/mnt/raid`, ~5.9 TiB) inside the
  notebook — one cell, runs once
- the notebook then measures the real speed with cold direct-IO tests (write 40 GiB, read
  it back once, then with 8 readers at once)
- optional GPU chapter: checks the GPU on the workers and measures RAM -> GPU copy speed
  (~8-11 GiB/s on a T4, so the GPU road is wider than the disk - the disk is the bottleneck)
- and finally: delete the cluster (it is real money)

The speed caps we measure come straight from Google's own table:
[Local SSD performance by number of disks](https://docs.cloud.google.com/compute/docs/disks/local-ssd#ssd-perf-disk-count)
— the "SCSI Local SSD performance" section, since Workbench/Dataproc currently always
attaches the disks as SCSI (the NVMe option is dropped by Terra's Workspace Manager; we
filed the fix upstream).

Network limits (only relevant for downloading data ONTO the machine - the local disk
numbers above never touch the network): https://cloud.google.com/compute/docs/gpus/gpu-network-bandwidth
For N1 with 1 GPU the network cap is min(2 Gbps x vCPUs, 32 Gbps) = at most 3.7 GiB/s,
reached already at 16 vCPUs (more CPUs do not add network, and do not change disk speed).

Cost: the whole recipe (2 workers with 16 local SSDs each) is about $4/hour, which would be
an equivalent of $2/h per worker/experiment. Delete the cluster when done (last step of the
notebook).

What this recipe achieves per worker (measured, us-central1, September 2026):

| test | speed per worker |
|---|---|
| default GCS bucket download | ~0.14 GiB/s |
| write 40 GiB (O_DIRECT) | ~3 GiB/s |
| read 40 GiB, one big-block reader | ~3.5 GiB/s steady |
| read 40 GiB, 8 readers at once | ~6 GiB/s (the official 16-disk SCSI cap) |

## Privileged-exec recipe (works on ANY Dataproc image, GPU or not)

Dataproc gives you no root and passwordless `sudo` is off, so root-level steps (mdadm, mounts,
GPU probes) run from a YARN task via a container escape. The old trick - `docker run` an EMPTY
imported image and exec the host's `/bin/bash` through a bind-mount of `/` - broke silently when
Google moved the OS image layout (`exec /host/bin/bash: no such file or directory`, exit 255).
The robust recipe, invariant across images:

1. **Never exec host binaries from an empty image.** Build a real image from the host's own system
   dirs once per worker: `tar -c -C / bin lib lib64 usr | docker import - hostimg` (~1-2 min,
   ~1-3 GB on the boot disk, cached afterwards, dies with the cluster).
2. **Escape = `docker run --privileged --pid=host -v /:/host -v /tmp:/tmp hostimg /bin/bash ...`
   then `nsenter -t 1 -m`** to run in the host mount namespace. Script is handed over via shared
   `/tmp`. Container exits after the script; nothing persists as a process.
3. **Run `envprobe` (Step 0) on any new cluster first** - it checks OS, docker server, builds
   hostimg, and smoke-tests the full escape (uid=0 x2 + INIMAGE_OK + NSENTER_OK) before you
   commit to long steps.
4. Guards self-heal: a dead `strip-*` container from a previous attempt is auto-removed; a
   running one is reported `ALREADY_RUNNING` and left alone.
