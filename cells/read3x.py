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
                       "/bin/bash -c '/usr/bin/nsenter -t 1 -m -- /bin/bash /tmp/r.sh'",
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
log = []
done = [False]
def beat():
    t0 = time.time()
    while not done[0]:
        time.sleep(10)
        print("HB %.0fs log=%d last=%s" % (time.time() - t0, len(log), (log[-1].strip()[:80] if log else "")), flush=True)
threading.Thread(target=beat, daemon=True).start()
for line in p.stdout:
    if line.startswith("RLAUNCH:"): print(line.rstrip()[:120], flush=True)
done[0] = True
p.wait(); print("watch LISTENER cell for R_ALLDONE x2", flush=True)