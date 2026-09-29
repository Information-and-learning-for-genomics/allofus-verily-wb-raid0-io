> **This file is a generated mirror of `local-ssd-raid0-io-test.ipynb`** - same content, formatted for comfortable reading and one-click copying on GitHub. The `.ipynb` is the runnable artifact; edit ONLY that, then regenerate this file (regeneration command at the bottom).

# Local SSD RAID0 speed test on Verily Workbench Dataproc

What this notebook does, in one sentence: it makes a Dataproc cluster, glues its local SSDs
into one fast volume (RAID0), and measures how fast you can really write and read it.

What we measured (so you know what to expect):

| what | speed (per worker) |
|---|---|
| default GCS bucket download | ~0.14 GiB/s |
| 4 local SSDs, plain read | ~1.4 GiB/s |
| 16 local SSDs, big block read | ~3.5 GiB/s |
| 16 local SSDs, 8 reads at once | ~6 GiB/s (the official SCSI maximum) |

## Step 1 - Create the cluster (run in your TERMINAL, not here)

```bash
wb resource create dataproc-cluster \
  --id=ssd-raid0 \
  --region=us-central1 \
  --quiet --format=JSON \
  --num-workers=2 \
  --worker-machine-type=n1-standard-16 \
  --worker-accelerator-type=nvidia-tesla-t4 \
  --worker-accelerator-count=1 \
  --worker-boot-disk-size=100 \
  --worker-num-local-ssds=16
```

The parameters:
- `--id` - name of the cluster. Use the same name in step 3 below.
- `--num-workers=2` - two worker machines. Each one gets its own local SSDs.
- `--worker-machine-type` - n1-standard-16 (16 CPUs, 60 GB RAM) is the cheapest shape
  allowed to carry a T4 (Google requires >=16 vCPUs per T4). More CPUs/RAM do NOT make
  the disks faster: the speed cap depends only on the number of local SSDs.
- `--worker-accelerator-type/-count` - one T4 GPU per worker (only needed for the GPU
  chapter at the end; leave both out for a pure disk test).
- `--worker-num-local-ssds=16` - THE parameter that matters: 375 GiB each, speed cap grows
  linearly with this count.

If a create ends in status ERROR with no reason: that is this tenant swallowing the real
cause (their code drops the error details); retry once - GPU provisioning fails
occasionally on capacity and a retry usually passes.

## Step 2 - Wait until it runs (terminal)

```bash
wb resource describe --id=ssd-raid0 --format=JSON | grep -E '"status"|"proxyUri"'
```

Wait for `"status": "RUNNING"` and copy the `proxyUri` link. That link opens JupyterLab
of the cluster. Note: the create/delete commands print nothing for 10-20 minutes. That is
normal, they are working, not frozen.

## Step 3 - Open JupyterLab, start THIS notebook

Open the proxyUri link. Create a new notebook and pick the **Python 3** kernel
(NOT "PySpark" - the PySpark kernel starts a background Spark app that causes noise).

Set the cluster name here - everything below uses it:

## Step 3b - Set the cluster name (used by nothing below automatically; the worker code derives names by itself)

Just a label so you can see what this notebook is pointed at:

```python
CLUSTER = "ssd-raid0"   # the --id you used in step 1
print("using cluster:", CLUSTER)
```

## How to get this notebook onto your cluster (pick one)

1. **Copy cells one by one** from this page - everything here is plain readable code.
   Copy carefully: if your browser or chat tool renders text as rich text/markdown, it can
   silently eat backslashes and dollar signs (the failure looks like "job failed" with a
   Python SyntaxError inside). If a cell fails with SyntaxError, re-copy it.
2. **Let the cluster fetch the code itself** (recommended): paste the tiny loader cell below
   ONCE, then run each step by changing one word. The step names match the file names in the
   `cells/` folder of this repository, so the code on GitHub and the code you run can never
   drift apart. Use `CELL = "listener"` only if you did not run the listener cell directly
   (the listener must run in the same kernel as everything else, and stay alive).

Names in order: `listener`, `clean`, `iface`, `stripe`, `fetchlogs`, `health`, `write40g`,
`read3x`, `queue8`, `gpu_check`, `gpu_h2d`, `gpu_fetch`, `stripdiag`.

```python
import urllib.request
CELL = "stripe"   # <- change this ONE word per step, then run the cell
URL = "https://raw.githubusercontent.com/Information-and-learning-for-genomics/allofus-verily-wb-raid0-io/main/cells/"
src = urllib.request.urlopen(URL + CELL + ".py", timeout=30).read().decode()
print("=== running step:", CELL, "|", len(src), "chars ===")
exec(compile(src, CELL, "exec"))
```

### Rescue cell: repair an old/corrupted copy of this notebook

If your JupyterLab still has a copy whose cells got mangled by copy-paste (garbled `$()`
fragments, SyntaxErrors that make no sense), run the cell below IN THAT OLD NOTEBOOK: it
downloads this notebook byte-for-byte from GitHub into your folder as `raid0-clean.ipynb`.
Then: shut down the old kernel, open `raid0-clean.ipynb`, pick the Python 3 kernel, and
continue from the top (listener first).

```python
import urllib.request
URL = "https://raw.githubusercontent.com/Information-and-learning-for-genomics/allofus-verily-wb-raid0-io/main/local-ssd-raid0-io-test.ipynb"
data = urllib.request.urlopen(URL, timeout=60).read()
open("raid0-clean.ipynb", "wb").write(data)
print("saved", len(data), "bytes as raid0-clean.ipynb - refresh the file browser and open it")
```

## Step 4 - Start the result printer (run first, keep it alive)

The slow tests run in the background ON THE WORKER MACHINES and send their progress lines
to this cell while they work. Start this and leave it alone (the cell stays busy - that is
the point). If you stop it, you will not see the test results.

```python
import threading, http.server, socketserver, time
try:
    LOGS
    print("listener already up; LOGS:", list(LOGS))
except NameError:
    LOGS = {}
    class HH(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            n = int(self.headers.get("Content-Length", 0))
            host = self.path.strip("/") or "unknown"
            LOGS[host] = self.rfile.read(n).decode(errors="replace")
            print(f"\n===== {host} @ {time.strftime('%H:%M:%S')} =====\n...{LOGS[host][-500:]}", flush=True)
            self.send_response(200); self.end_headers()
        def log_message(self, *a): pass
    class SRV(socketserver.ThreadingTCPServer):
        allow_reuse_address = True; daemon_threads = True
    threading.Thread(target=SRV(("0.0.0.0", 18888), HH).serve_forever, daemon=True).start()
    print("listener started fresh")
```

## Step 5 - Clean up stray Spark apps (optional, silences noise)

Kills leftover Spark apps from earlier runs/sessions so they cannot hold the workers. Harmless if it says nothing.

```python
import subprocess, json
r = subprocess.run(["sudo","curl","-s","-m10","http://localhost:8088/ws/v1/cluster/apps"], capture_output=True, text=True, timeout=30)
for a in json.loads(r.stdout)["apps"]["app"]:
    if a["state"] in ("ACCEPTED", "RUNNING"):
        k = subprocess.run(f"sudo -u yarn yarn application -kill {a['id']}", shell=True, capture_output=True, text=True, timeout=60)
        print("KILLED", a["id"], "->", (k.stdout or k.stderr).strip()[-80:], flush=True)
    else:
        print(a["id"], a["state"], a["finalStatus"])
```

## Step 6 - Look at the disks (interface check)

Runs a small job on both workers and prints the disk list per worker. Expect 16 lines of `sd... scsi LOCAL_SSD 375G` and `no-nvme-disks` - this tenant always attaches local SSDs as SCSI (the NVMe option is dropped by the platform on the way through). If only ONE worker prints, re-run this cell - YARN sometimes stacks both executors on one machine, that is noise, not a finding.

```python
import subprocess
JOB = '''
import socket, subprocess, os
def probe():
    h = os.uname().nodename
    ls = "; ".join(subprocess.run("lsblk -d -o NAME,TRAN,MODEL,SIZE | grep 375G", shell=True, capture_output=True, text=True).stdout.splitlines())
    nv = subprocess.run("ls /dev/nvme*n1 2>/dev/null || echo no-nvme-disks", shell=True, capture_output=True, text=True).stdout.strip()
    mt = subprocess.run('curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/machine-type', shell=True, capture_output=True, text=True).stdout
    return "IFACE " + h + " | " + mt.split("/")[-1].strip() + " | 375G-devs: " + ls + " | " + nv
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    sp = SparkSession.builder.appName("iface").getOrCreate()
    for x in sorted(set(sp.sparkContext.parallelize(range(8), 8).map(lambda _: probe()).collect())):
        print(x, flush=True)
    sp.stop()
'''
open("/tmp/job_iface.py", "w").write(JOB)
p = subprocess.Popen(["spark-submit","--master","yarn","--deploy-mode","client",
   "--conf","spark.executor.instances=2","--conf","spark.executor.memory=1g","/tmp/job_iface.py"],
   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
log = []
for line in p.stdout:
    log.append(line)
    if line.startswith("IFACE"): print(line.rstrip()[:250], flush=True)
rc = p.wait()
print("rc:", rc, flush=True)
if rc != 0:
    print("--- job FAILED - last 25 lines of its output ---")
    print("".join(log[-25:]))
```

## Step 7 - Glue the disks together (RAID0 stripe)

What this cell does on each worker (through the worker's own Docker, as root):
1. stops the Hadoop/Yarn services that hold the disks,
2. finds every 375-GiB disk by SIZE (so the boot disk can never be touched),
3. glues all 16 into one device with `dmsetup` (stripe = classic RAID0),
4. makes one big ext4 filesystem, mounts it at `/mnt/raid` (~5.9 TiB),
5. fixes the Hadoop folder owners and restarts the services.

It runs once; running it again says ALREADY and does nothing. Expect
`LAUNCH_RESULT ... rc=0` for both workers, then look at step 8.

```python
import subprocess
STRIP = r"""#!/bin/bash
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
exec >> /tmp/strip.log 2>&1
set -x
shopt -s nullglob
H=$(hostname); MGR="${H%%-w-*}-m.$(hostname -d)"
post() { curl -m5 -s -X POST --data-binary @/tmp/strip.log "http://$MGR:18888/$H" || true; }
date; echo "STRIP START $H"; post
exec 9>/tmp/strip.lock; flock -n 9 || { echo ALREADY_RUNNING; exit 0; }
LDIRS=$(cut -f2 -d' ' /proc/mounts | grep "^/mnt/" | tr '\n' ' ')
echo "LDIRS=$LDIRS"; post
DISKS=""
for b in /sys/block/sd* /sys/block/nvme*n1; do S=$(cat $b/size 2>/dev/null); if [ -n "$S" ] && [ "$S" -gt 700000000 ]; then DISKS="$DISKS /dev/$(basename $b)"; fi; done
N=$(echo $DISKS | wc -w); echo "DISKS=$DISKS N=$N"
[ "$N" -lt 16 ] && { echo WANT_16_GOT_$N; post; exit 1; }
SZ=$(for d in $DISKS; do cat /sys/block/$(basename $d)/size; done | sort -n | head -1)
LEN=$((SZ*N)); echo "SZ=$SZ LEN=$LEN"; post
systemctl stop hadoop-yarn-nodemanager hadoop-hdfs-datanode 2>&1; sleep 3
for m in $LDIRS; do umount -l $m; done
sleep 2
ARGS=""; for d in $DISKS; do ARGS="$ARGS $d 0"; done
OK=""
for T in striped stripe; do
  dmsetup create ssdraid --table "0 $LEN $T $N 256 $ARGS" && { echo DM_OK=$T; OK=1; break; }
  dmsetup remove ssdraid 2>/dev/null
done
post
if [ -n "$OK" ] && dmsetup info ssdraid >/dev/null 2>&1; then
  echo MKFS_BEGIN; post
  mkfs.ext4 -F -q /dev/mapper/ssdraid && mkdir -p /mnt/raid && mount /dev/mapper/ssdraid /mnt/raid && chmod 1777 /mnt/raid && echo ALLDONE_6T
  df -h /mnt/raid
else
  echo DM_FAIL
fi
for m in $LDIRS; do
  mkdir -p $m/hadoop/dfs/data $m/hadoop/yarn/nm-local-dir
  chown -R hdfs:hadoop $m/hadoop/dfs
  chown -R yarn:yarn $m/hadoop/yarn
  chown root:root $m; chmod 0755 $m
done
echo DIRS_FIXED; post
systemctl restart hadoop-hdfs-datanode 2>&1 || true
systemctl restart hadoop-yarn-nodemanager 2>&1 || true
sleep 15
echo "DN=$(systemctl is-active hadoop-hdfs-datanode 2>&1) NM=$(systemctl is-active hadoop-yarn-nodemanager 2>&1)"
echo STRIP_DONE; date; post
"""
JOB = '''
import socket, subprocess
STRIP = open("/tmp/strip-host.sh").read()
def launch():
    h = socket.gethostname(); name = "strip-" + h.split(".")[0]
    if subprocess.run(["bash","-c","docker ps -a --format '{{.Names}}' | grep -qx " + name],
                      capture_output=True).returncode == 0:
        return (h, "ALREADY")
    open("/tmp/strip-host.sh", "w").write(STRIP)
    subprocess.run("docker image inspect hostimg >/dev/null 2>&1 || "
                       "tar -c -C / bin lib lib64 usr 2>/dev/null | docker import - hostimg",
               shell=True, capture_output=True, timeout=600)
    r = subprocess.run("docker run -d --name " + name + " --privileged --pid=host --uts=host --network=host --ipc=host "
                       "-v /:/host -v /lib64:/lib64 -v /usr:/usr -v /tmp:/tmp hostimg "
                       "/host/bin/bash -c '/usr/bin/nsenter -t 1 -m -- /bin/bash /tmp/strip-host.sh'",
                       shell=True, capture_output=True, text=True)
    return (h, "rc=%s %s" % (r.returncode, r.stderr[-80:]))
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    sp = SparkSession.builder.appName("strip-launch").getOrCreate()
    for x in sorted(set(sp.sparkContext.parallelize(range(16), 16).map(lambda _: launch()).collect())):
        print("LAUNCH_RESULT:", x, flush=True)
    sp.stop()
'''
open("/tmp/strip-host.sh", "w").write(STRIP)
open("/tmp/job_strip.py", "w").write(JOB)
cmd = ["spark-submit", "--master", "yarn", "--deploy-mode", "client",
       "--conf", "spark.executor.instances=2",
       "--conf", "spark.excludeOnFailure.enabled=false",
       "--conf", "spark.yarn.executor.launch.excludeOnFailure.enabled=false",
       "--conf", "spark.executor.maxNumFailures=1000",
       "--conf", "spark.executor.memory=1g", "/tmp/job_strip.py"]
p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
for line in p.stdout:
    if line[:3] != "26/" and not line.startswith("\tat "): print(line.rstrip()[:300], flush=True)
print("rc:", p.wait(), flush=True)
```

## Step 8 - Check that the stripe is healthy

```python
import subprocess
JOB = '''
import socket, subprocess, os
def probe():
    h = socket.gethostname().split(".")[0]
    def sh(c):
        r = subprocess.run(c, shell=True, capture_output=True, text=True, timeout=30)
        return ((r.stdout or "") + (r.stderr or ""))[-1200:]
    log = open("/tmp/strip.log").read()[-1800:] if os.path.exists("/tmp/strip.log") else "MISSING strip.log (script never ran)"
    docker = sh("docker ps -a --format '{{.Names}} {{.Status}}' | grep strip || echo no-strip-container")
    df = sh("df -h /mnt/raid | tail -1")
    return "\\n##### " + h + " #####\\n" + log + "\\nDOCKER: " + docker + "\\nDF: " + df
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    sp = SparkSession.builder.appName("fetch").getOrCreate()
    out = sp.sparkContext.parallelize(range(16), 16).map(lambda _: probe()).collect()
    for x in sorted(set(out)): print(x, flush=True)
    sp.stop()
'''
open("/tmp/job_fetch.py", "w").write(JOB)
p = subprocess.Popen(["spark-submit", "--master", "yarn", "--deploy-mode", "client",
                      "--conf", "spark.executor.instances=2", "--conf", "spark.executor.memory=1g",
                      "/tmp/job_fetch.py"],
                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
for line in p.stdout:
    if line[:3] != "26/" and not line.startswith("\tat "): print(line.rstrip()[:300], flush=True)
print("rc:", p.wait(), flush=True)
```

```python
import subprocess, json
r = subprocess.run(["sudo","curl","-s","-m10","http://localhost:8088/ws/v1/cluster/nodes"], capture_output=True, text=True, timeout=30)
for x in json.loads(r.stdout)["nodes"]["node"]:
    print("NODE:", x["nodeHostName"].split(".")[0], "|", x["state"], "|", x["healthReport"][:100] or "OK")
```

## Step 9 - Write test: 40 GiB per worker

Writes 40 GiB of zeros straight to the disks (bypassing the Linux page cache, so the
number is real disk speed, not RAM). Progress lines go to the step-4 cell.
Watch for `W_DONE <worker> <seconds> <GiB/s>`. With 16 disks expect ~3 GiB/s.

```python
import subprocess, random
FR = r"""#!/bin/bash
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
H=$(hostname); MGR="${H%%-w-*}-m.$(hostname -d)"
post() { curl -m5 -s -X POST --data-binary "$1" "http://$MGR:18888/w-$H" || true; }
exec 9>/tmp/w.lock; flock -n 9 || { post "W_SKIPLOCK $H"; exit 0; }
F=/mnt/raid/benchfile; T=$((40*1024*1024*1024))
if [ "$(stat -c %s "$F" 2>/dev/null || echo 0)" = "$T" ]; then post "W_ALREADY $H"; exit 0; fi
rm -f "$F"; post "W_START $H"; t0=$(date +%s)
dd if=/dev/zero of="$F" bs=4M count=10240 oflag=direct status=noxfer 2>/tmp/w.err & pid=$!
while kill -0 $pid 2>/dev/null; do sleep 10
  wb=$(awk '/^write_bytes/{print $2}' /proc/$pid/io 2>/dev/null); e=$(( $(date +%s)-t0 ))
  [ -n "$wb" ] && post "W $H ${e}s $(awk "BEGIN{printf \"%.2f\", $wb/1073741824}") GiB ~$(awk "BEGIN{printf \"%.2f\", $wb/1073741824/(($e)+1)}") GiB/s"
done; wait $pid; e=$(( $(date +%s)-t0 ))
post "W_DONE $H ${e}s $(awk "BEGIN{printf \"%.2f\", 40/$e}") GiB/s"
"""
JOB = '''
import socket, subprocess
FR = open("/tmp/w.sh").read()
def launch():
    h = socket.gethostname().split(".")[0]
    open("/tmp/w.sh", "w").write(FR)
    r = subprocess.run("docker run -d --name w-" + SUFFIX + "-" + h + " --privileged --pid=host --uts=host --network=host --ipc=host "
                       "-v /:/host -v /lib64:/lib64 -v /lib:/lib -v /usr:/usr -v /tmp:/tmp hostimg "
                       "/host/bin/bash -c '/usr/bin/nsenter -t 1 -m -- /bin/bash /tmp/w.sh'",
                       shell=True, capture_output=True, text=True)
    return "WOK " + h + " rc=" + str(r.returncode)
if __name__ == "__main__":
    import os
    SUFFIX = os.environ["SFX"]
    from pyspark.sql import SparkSession
    sp = SparkSession.builder.appName("w-launch").getOrCreate()
    for x in sorted(set(sp.sparkContext.parallelize(range(16), 16).map(lambda _: launch()).collect())):
        print("WLAUNCH:", x, flush=True)
    sp.stop()
'''
open("/tmp/w.sh", "w").write(FR); open("/tmp/job_w.py", "w").write(JOB)
import os
env = dict(os.environ, SFX="%04x" % random.randrange(65536))
p = subprocess.Popen(["spark-submit","--master","yarn","--deploy-mode","client",
   "--conf","spark.executor.instances=2","--conf","spark.executor.memory=1g","/tmp/job_w.py"],
   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env)
for line in p.stdout:
    if line.startswith("WLAUNCH:"): print(line.rstrip()[:120], flush=True)
p.wait(); print("watch LISTENER cell for W_DONE x2", flush=True)
```

## Step 10 - Read tests

**10a. Same file, 3 times in a row.** Reads the 40 GiB back three times with 4 MiB
requests. This separates a short speed "burst" from the real steady speed. Look for
`R_ALLDONE <worker> total <s>s passes: a b c`. Expect ~3.4 GiB/s, same on all passes.

```python
import subprocess, random
FR = r"""#!/bin/bash
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
H=$(hostname); MGR="${H%%-w-*}-m.$(hostname -d)"
post() { curl -m5 -s -X POST --data-binary "$1" "http://$MGR:18888/r-$H" || true; }
exec 9>/tmp/r.lock; flock -n 9 || { post "R_SKIPLOCK $H"; exit 0; }
F=/mnt/raid/benchfile
if [ "$(stat -c %s "$F" 2>/dev/null || echo 0)" != "$((40*1024*1024*1024))" ]; then post "R_NOFILE $H"; exit 0; fi
SUM=0; ALL=""
for p in 1 2 3; do
  post "R_P${p}_START $H"; t0=$(date +%s)
  dd if="$F" of=/dev/null bs=4M count=10240 iflag=direct status=noxfer 2>/tmp/r.err & pid=$!
  ( while sleep 5; do kill -0 $pid 2>/dev/null || break
      rb=$(awk '/^read_bytes/{print $2}' /proc/$pid/io 2>/dev/null); e=$(( $(date +%s)-t0 ))
      [ -n "$rb" ] && post "R_P${p} $H ${e}s $(awk "BEGIN{printf \"%.2f\", $rb/1073741824}") GiB ~$(awk "BEGIN{printf \"%.2f\", $rb/1073741824/(($e)+1)}") GiB/s"
    done ) & mon=$!
  wait $pid; kill $mon 2>/dev/null; e=$(( $(date +%s)-t0 )); r=$(awk "BEGIN{printf \"%.2f\", 40/($e)}")
  post "R_P${p}DONE $H ${e}s ${r} GiB/s"; SUM=$((SUM+e)); ALL="$ALL ${r}"
done
post "R_ALLDONE $H total ${SUM}s passes:$ALL"
"""
JOB = '''
import socket, subprocess
FR = open("/tmp/r.sh").read()
def launch():
    h = socket.gethostname().split(".")[0]
    open("/tmp/r.sh", "w").write(FR)
    r = subprocess.run("docker run -d --name r-" + SUFFIX + "-" + h + " --privileged --pid=host --uts=host --network=host --ipc=host "
                       "-v /:/host -v /lib64:/lib64 -v /lib:/lib -v /usr:/usr -v /tmp:/tmp hostimg "
                       "/host/bin/bash -c '/usr/bin/nsenter -t 1 -m -- /bin/bash /tmp/r.sh'",
                       shell=True, capture_output=True, text=True)
    return "ROK " + h + " rc=" + str(r.returncode)
if __name__ == "__main__":
    import os
    SUFFIX = os.environ["SFX"]
    from pyspark.sql import SparkSession
    sp = SparkSession.builder.appName("r-launch").getOrCreate()
    for x in sorted(set(sp.sparkContext.parallelize(range(16), 16).map(lambda _: launch()).collect())):
        print("RLAUNCH:", x, flush=True)
    sp.stop()
'''
open("/tmp/r.sh", "w").write(FR); open("/tmp/job_r.py", "w").write(JOB)
import os
env = dict(os.environ, SFX="%04x" % random.randrange(65536))
p = subprocess.Popen(["spark-submit","--master","yarn","--deploy-mode","client",
   "--conf","spark.executor.instances=2","--conf","spark.executor.memory=1g","/tmp/job_r.py"],
   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env)
for line in p.stdout:
    if line.startswith("RLAUNCH:"): print(line.rstrip()[:120], flush=True)
p.wait(); print("watch LISTENER cell for R_ALLDONE x2", flush=True)
```

**10b. 8 readers at once (deep queue).** One reader waits for every request to finish
before the next one, which leaves the 16 disks mostly idle. Eight readers keep ~128
requests in flight, which is what Google's speed table assumes. This is also the shape
that kvikio (storage->GPU streaming) uses. Expect ~6 GiB/s - the official 16-disk SCSI
maximum. If you see this number, your GPU reader can realistically eat 6 GiB/s per node.

```python
import subprocess, random
FR = r"""#!/bin/bash
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
H=$(hostname); MGR="${H%%-w-*}-m.$(hostname -d)"
post() { curl -m5 -s -X POST --data-binary "$1" "http://$MGR:18888/r2-$H" || true; }
exec 9>/tmp/r2.lock; flock -n 9 || { post "R2_SKIPLOCK $H"; exit 0; }
F=/mnt/raid/benchfile
if [ "$(stat -c %s "$F" 2>/dev/null || echo 0)" != "$((40*1024*1024*1024))" ]; then post "R2_NOFILE $H"; exit 0; fi
post "R2_START $H 8 streams bs=4M"; t0=$(date +%s)
PIDS=""
for i in 0 1 2 3 4 5 6 7; do
  dd if="$F" of=/dev/null bs=4M count=1280 skip=$((i*1280)) iflag=direct status=noxfer 2>/dev/null &
  PIDS="$PIDS $!"
done
( while sleep 5; do
      TOT=0
      for pid in $PIDS; do
        rb=$(awk '/^read_bytes/{print $2}' /proc/$pid/io 2>/dev/null); TOT=$((TOT + ${rb:-0}))
      done
      e=$(( $(date +%s)-t0 ))
      post "R2 $H ${e}s $(awk "BEGIN{printf \"%.2f\", $TOT/1073741824}") GiB ~$(awk "BEGIN{printf \"%.2f\", $TOT/1073741824/(($e)+1)}") GiB/s"
    done ) & mon=$!
for pid in $PIDS; do wait $pid; done
kill $mon 2>/dev/null; e=$(( $(date +%s)-t0 ))
post "R2_DONE $H ${e}s $(awk "BEGIN{printf \"%.2f\", 40/($e)}") GiB/s aggregate-8-streams"
"""
JOB = '''
import socket, subprocess
FR = open("/tmp/r2.sh").read()
def launch():
    h = socket.gethostname().split(".")[0]
    open("/tmp/r2.sh", "w").write(FR)
    r = subprocess.run("docker run -d --name r2-" + SUFFIX + "-" + h + " --privileged --pid=host --uts=host --network=host --ipc=host "
                       "-v /:/host -v /lib64:/lib64 -v /lib:/lib -v /usr:/usr -v /tmp:/tmp hostimg "
                       "/host/bin/bash -c '/usr/bin/nsenter -t 1 -m -- /bin/bash /tmp/r2.sh'",
                       shell=True, capture_output=True, text=True)
    return "R2OK " + h + " rc=" + str(r.returncode)
if __name__ == "__main__":
    import os
    SUFFIX = os.environ["SFX"]
    from pyspark.sql import SparkSession
    sp = SparkSession.builder.appName("r2-launch").getOrCreate()
    for x in sorted(set(sp.sparkContext.parallelize(range(16), 16).map(lambda _: launch()).collect())):
        print("R2LAUNCH:", x, flush=True)
    sp.stop()
'''
open("/tmp/r2.sh", "w").write(FR); open("/tmp/job_r2.py", "w").write(JOB)
import os
env = dict(os.environ, SFX="%04x" % random.randrange(65536))
p = subprocess.Popen(["spark-submit","--master","yarn","--deploy-mode","client",
   "--conf","spark.executor.instances=2","--conf","spark.executor.memory=1g","/tmp/job_r2.py"],
   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env)
for line in p.stdout:
    if line.startswith("R2LAUNCH:"): print(line.rstrip()[:120], flush=True)
p.wait(); print("watch LISTENER cell for R2_DONE x2", flush=True)
```

## Step 11 - Delete everything (TERMINAL - do this, it is real money)

```bash
wb resource delete --id=ssd-raid0 --quiet
wb resource list | grep ssd     # must print nothing
```

Deleting also prints nothing for 10-20 minutes, then it is gone. If you leave the cluster
"just for tonight" it bills roughly $4/hour.

## Diagnostic step: why did a launch fail? (stripdiag)

If a step's containers show `Exited (255)` or logs say `MISSING`, run the cell below
(or set the loader to `CELL = "stripdiag"`). It runs the SAME privileged host-escape with
harmless echo probes on both workers and prints each worker's stripe-container log —
that output names the actual failure.

```python
# DIAGNOSTIC: why did the stripe container exit 255 on the workers?
# Runs the SAME docker escape with echo probes and shows container logs.
import subprocess
JOB = '''
import socket, subprocess
def sh(c):
    r = subprocess.run(c, shell=True, capture_output=True, text=True, timeout=120)
    return ((r.stdout or "") + (r.stderr or ""))[-900:]
def ensure_img():
    subprocess.run("docker image inspect hostimg >/dev/null 2>&1 || "
                   "tar -c -C / bin lib lib64 usr 2>/dev/null | docker import - hostimg",
                   shell=True, capture_output=True, timeout=600)
def probe():
    ensure_img()
    h = socket.gethostname().split(".")[0]
    out = ["DIAG " + h]
    out.append(sh("docker logs strip-" + h + " 2>&1 | tail -20"))
    out.append(sh("ls -la /tmp/strip* 2>&1 | tail -4"))
    out.append(sh("docker run --rm --privileged --pid=host --uts=host --network=host "
                  "--ipc=host -v /:/host -v /lib64:/lib64 -v /lib:/lib -v /usr:/usr "
                  "-v /tmp:/tmp -v /dev:/dev hostimg /bin/bash -c "
                  "'echo INSIDE_OK; /usr/bin/nsenter -t 1 -m -- /bin/echo NSENTER_OK'"))
    return " ;; ".join(out)
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    sp = SparkSession.builder.appName("stripdiag").getOrCreate()
    for x in sorted(set(sp.sparkContext.parallelize(range(8), 8).map(lambda _: probe()).collect())):
        print(x, flush=True)
    sp.stop()
'''
open("/tmp/job_stripdiag.py", "w").write(JOB)
p = subprocess.Popen(["spark-submit", "--master", "yarn", "--deploy-mode", "client",
   "--conf", "spark.executor.instances=2", "--conf", "spark.executor.memory=1g",
   "/tmp/job_stripdiag.py"],
   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
log = []
for line in p.stdout:
    log.append(line)
    if line.startswith("DIAG"): print(line.rstrip(), flush=True)
rc = p.wait()
print("rc:", rc, flush=True)
if rc != 0:
    print("--- job FAILED - last 25 lines ---")
    print("".join(log[-25:]))
```

## GPU chapter (optional): the second pipe - RAM into the GPU

The disk tests above never touch the GPU. These three cells check the GPU is visible on
both workers and measure the host-RAM -> GPU copy speed, the second hop of the pipeline
`local disk -> RAM -> GPU`. A T4 sits on PCIe Gen3 x16, so expect roughly 8-11 GiB/s -
faster than the ~6 GiB/s the 16-disk RAID0 can feed it, which is the point: the disk,
not the GPU road, is the narrow pipe.

Note: this chapter was added 2026-09-29 and its numbers had not been measured in the wild
yet; the method (same worker channel as the disk tests) is the proven one. Run GPU-1,
then GPU-2, then GPU-3 after the H2_DONE heartbeats appear in the listener cell.

```python
# GPU-1: is the GPU visible and healthy on every worker? Output comes back into this
# cell. If only one worker appears, just re-run (YARN may stack both executors on one
# worker; a few re-runs reach both).
import subprocess
JOB = '''
import socket, subprocess
def ensure_img():
    subprocess.run("docker image inspect hostimg >/dev/null 2>&1 || "
                   "tar -c -C / bin lib lib64 usr 2>/dev/null | docker import - hostimg",
                   shell=True, capture_output=True, timeout=600)
def probe():
    ensure_img()
    h = socket.gethostname().split(".")[0]
    cmd = ("docker run --rm --privileged --pid=host --uts=host --network=host --ipc=host "
           "-v /:/host -v /lib64:/lib64 -v /lib:/lib -v /usr:/usr -v /tmp:/tmp -v /dev:/dev hostimg "
           "/host/bin/bash -c '/usr/bin/nsenter -t 1 -m -- nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv'")
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
    return "GPU " + h + " rc=" + str(r.returncode) + " | " + ((r.stdout or "") + (r.stderr or ""))[-300:]
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    sp = SparkSession.builder.appName("gpu-check").getOrCreate()
    for x in sorted(set(sp.sparkContext.parallelize(range(4), 4).map(lambda _: probe()).collect())):
        print("GPUCHECK:", x, flush=True)
    sp.stop()
'''
open("/tmp/job_gpu.py", "w").write(JOB)
p = subprocess.Popen(["spark-submit","--master","yarn","--deploy-mode","client",
   "--conf","spark.executor.instances=2","--conf","spark.executor.memory=1g","/tmp/job_gpu.py"],
   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
log = []
for line in p.stdout:
    log.append(line)
    if line.startswith("GPUCHECK:"): print(line.rstrip()[:250], flush=True)
rc = p.wait()
print("rc:", rc, flush=True)
if rc != 0:
    print("--- job FAILED - last 25 lines of its output ---")
    print("".join(log[-25:]))
```

```python
# GPU-2: RAM -> GPU copy speed on both workers: 30 seconds of 150 MiB pinned copies on
# a T4 (PCIe Gen3). The FIRST run also pip-installs cupy on the workers (can take a few
# minutes) - that is normal. Heartbeats (H2 ...) land in the step-4 listener cell while
# it runs; GPU-3 collects the final numbers.
import subprocess, random, os
SFX = "%04x" % random.randrange(65536)
FR = r"""#!/bin/bash
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
H=$(hostname); MGR="${H%%-w-*}-m.$(hostname -d)"
post() { curl -m5 -s -X POST --data-binary "$1" "http://$MGR:18888/h2-$H" || true; }
exec 9>/tmp/h2dTOK.lock; flock -n 9 || { post "H2_SKIPLOCK $H"; exit 0; }
post "H2_INSTALL $H first run pip-installs cupy, can take minutes"
python3 -m pip install --quiet cupy-cuda12x >/tmp/h2d_pipTOK.log 2>&1 || { post "H2_PIPFAIL $H see /tmp/h2d_pipTOK.log"; exit 0; }
post "H2_RUN $H 30s pinned-copy loop started"
python3 /tmp/h2d_progTOK.py $MGR $H >/tmp/h2d_outTOK.log 2>&1
post "H2_DONE $H rc=$? last: $(tail -1 /tmp/h2d_outTOK.log)"
""".replace("TOK", SFX)
PROG = r"""import sys, time, urllib.request
import cupy as cp
mgr, h = sys.argv[1], sys.argv[2]
def post(msg):
    try:
        urllib.request.urlopen("http://" + mgr + ":18888/hb-" + h, data=msg.encode(), timeout=5)
    except Exception:
        pass
nb = 150 * 1024 * 1024
pin = cp.cuda.alloc_pinned_memory(nb)
host = cp.ndarray(nb // 4, cp.float32, cp.cuda.MemoryPointer(cp.cuda.UnownedMemory(pin.ptr, nb, pin), 0))
dev = cp.empty(nb // 4, dtype=cp.float32)
start = time.time(); n = 0
while time.time() - start < 30:
    dev[...] = host
    n += 1
    if n % 10 == 0:
        gib = n * 150 / 1024
        post("H2 %s %.1fs %.2f GiB ~%.2f GiB/s" % (h, time.time() - start, gib, gib / (time.time() - start)))
"""
open("/tmp/h2d_prog" + SFX + ".py", "w").write(PROG)
open("/tmp/h2d" + SFX + ".sh", "w").write(FR)
JOB = '''
import socket, subprocess
FR = open("/tmp/h2dTOK.sh").read()
PG = open("/tmp/h2d_progTOK.py").read()
def launch():
    h = socket.gethostname().split(".")[0]
    open("/tmp/h2dTOK.sh", "w").write(FR)
    open("/tmp/h2d_progTOK.py", "w").write(PG)
    r = subprocess.run("docker run -d --name h2-TOK-" + h + " --privileged --pid=host --uts=host --network=host --ipc=host "
                       "-v /:/host -v /lib64:/lib64 -v /lib:/lib -v /usr:/usr -v /tmp:/tmp -v /dev:/dev hostimg "
                       "/host/bin/bash -c '/usr/bin/nsenter -t 1 -m -- /bin/bash /tmp/h2dTOK.sh'",
                       shell=True, capture_output=True, text=True)
    return "H2START " + h + " rc=" + str(r.returncode)
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    sp = SparkSession.builder.appName("h2d-launch").getOrCreate()
    for x in sorted(set(sp.sparkContext.parallelize(range(16), 16).map(lambda _: launch()).collect())):
        print("H2LAUNCH:", x, flush=True)
    sp.stop()
'''.replace("TOK", SFX)
open("/tmp/job_h2d.py", "w").write(JOB)
p = subprocess.Popen(["spark-submit","--master","yarn","--deploy-mode","client",
   "--conf","spark.executor.instances=2","--conf","spark.executor.memory=1g","/tmp/job_h2d.py"],
   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
log = []
for line in p.stdout:
    log.append(line)
    if line.startswith("H2LAUNCH:"): print(line.rstrip()[:160], flush=True)
rc = p.wait()
print("rc:", rc, " SFX:", SFX, flush=True)
if rc != 0:
    print("--- job FAILED - last 25 lines of its output ---")
    print("".join(log[-25:]))
```

```python
# GPU-3: collect the RAM -> GPU results from both workers (uses SFX set by GPU-2, so
# run AFTER GPU-2 in the same kernel). Wait until you have seen
# H2_DONE for both workers in the listener cell (first run: after the pip install).
# Expect ~8-11 GiB/s per worker on a T4. Re-run this cell if a worker says "no result yet".
import subprocess
JOB = '''
import socket, subprocess
def probe():
    h = socket.gethostname().split(".")[0]
    f = "/tmp/h2d_outTOK.log"
    r = subprocess.run("tail -3 " + f + " 2>/dev/null || echo NOFILE", shell=True, capture_output=True, text=True)
    return "H2 " + h + " | " + r.stdout.replace(chr(10), " ;; ")
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    sp = SparkSession.builder.appName("h2d-fetch").getOrCreate()
    for x in sorted(set(sp.sparkContext.parallelize(range(8), 8).map(lambda _: probe()).collect())):
        print("H2FETCH:", x, flush=True)
    sp.stop()
'''.replace("TOK", SFX)
open("/tmp/job_h2d_fetch.py", "w").write(JOB)
p = subprocess.Popen(["spark-submit","--master","yarn","--deploy-mode","client",
   "--conf","spark.executor.instances=2","--conf","spark.executor.memory=1g","/tmp/job_h2d_fetch.py"],
   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
log = []
for line in p.stdout:
    log.append(line)
    if line.startswith("H2FETCH:"): print(line.rstrip()[:250], flush=True)
rc = p.wait()
print("rc:", rc, flush=True)
if rc != 0:
    print("--- job FAILED - last 25 lines of its output ---")
    print("".join(log[-25:]))
```

## What the numbers mean for a GPU streaming design

- One worker with 16 local SSDs gives ~6 GiB/s of steady raw reads when the reader keeps
  many requests in flight. A single slow reader sees only ~1.2-3.5 GiB/s - same disks,
  same machine, only the queue depth differs.
- Network (only for getting data onto the machine): N1 with 1 GPU = min(2 Gbps x vCPUs,
  32 Gbps) = up to 3.7 GiB/s, already maxed at 16 vCPUs
  (https://cloud.google.com/compute/docs/gpus/gpu-network-bandwidth). More CPUs do not
  change the local-disk numbers at all.
- The GPU slot is not the limit: a T4's PCIe slot passes ~11 GiB/s from RAM to GPU,
  comfortably above the 6 GiB/s the disks can feed it.
- The 16-disk trick only works on the plain CPU workers and on N1+GPU machines (N1 with
  T4/V100 accepts 16+ local SSDs). The L4 "G2" machines accept at most 8 local SSDs
  (~2.5 GiB/s), and A100 "A2" also stop at 8. If you need GPU + big local IO on one
  box, N1+T4 is the only cheap combination that can attach 16-24 local SSDs.
- NVMe interface would raise the per-disk-count caps further, but Terra's Workspace
  Manager currently drops the `localSsdInterface` argument (one missing line upstream,
  we filed it). Until that is fixed, everything here is the SCSI path.
- Price: this whole recipe (2 workers + 16 local SSDs each) is about $4/hour, which would be
  an equivalent of $2/h per worker/experiment.
  24 disks per worker (9,360 MiB/s row) instead of 16 barely changes the bill.
  Speed caps: https://docs.cloud.google.com/compute/docs/disks/local-ssd#ssd-perf-disk-count

