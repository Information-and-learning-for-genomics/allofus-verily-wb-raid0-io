# allofus-verily-wb-raid0-io

How to get ~6 GiB/s of raw local disk speed per worker on a Verily Workbench (Terra)
Dataproc cluster, and how to check it yourself. One notebook, start to finish:
create cluster -> stripe the 16 local SSDs into RAID0 -> measure write and read speed ->
delete the cluster.

Open `local-ssd-raid0-io-test.ipynb` and run it top to bottom. The first cell explains
what you need and what numbers to expect. Everything runs inside the JupyterLab of the
cluster itself; the only terminal steps are creating and deleting the cluster.

Measured with this exact recipe (2x n2-standard-32 workers, 16x375 GiB local SSDs,
us-central1, September 2026):

| test | speed per worker |
|---|---|
| write 40 GiB (O_DIRECT) | ~3 GiB/s |
| read 40 GiB, one big-block reader | ~3.5 GiB/s steady |
| read 40 GiB, 8 readers at once | ~6 GiB/s (the official 16-disk SCSI cap) |

The official disk speed caps come from Google's own table:
https://docs.cloud.google.com/compute/docs/disks/local-ssd#ssd-perf-disk-count
("SCSI Local SSD performance" / "NVMe Local SSD performance" sections).

Cost: the whole recipe (2 workers + 16 local SSDs each) is about $4/hour, so one full
run - create, benchmark, delete - is roughly $4. Delete the cluster when done
(last step of the notebook).
