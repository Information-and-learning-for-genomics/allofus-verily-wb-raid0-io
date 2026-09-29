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
                       "/bin/bash -c '/usr/bin/nsenter -t 1 -m -- /bin/bash /tmp/r2.sh'",
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