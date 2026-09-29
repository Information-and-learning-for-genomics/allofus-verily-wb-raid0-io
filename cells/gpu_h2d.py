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
                       "/bin/bash -c '/usr/bin/nsenter -t 1 -m -- /bin/bash /tmp/h2dTOK.sh'",
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
done = [False]
def beat():
    t0 = time.time()
    while not done[0]:
        time.sleep(10)
        print("HB %.0fs log=%d last=%s" % (time.time() - t0, len(log), (log[-1].strip()[:80] if log else "")), flush=True)
threading.Thread(target=beat, daemon=True).start()
for line in p.stdout:
    log.append(line)
    if line.startswith("H2LAUNCH:"): print(line.rstrip()[:160], flush=True)
done[0] = True
rc = p.wait()
print("rc:", rc, " SFX:", SFX, flush=True)
if rc != 0:
    print("--- job FAILED - last 25 lines of its output ---")
    print("".join(log[-25:]))
