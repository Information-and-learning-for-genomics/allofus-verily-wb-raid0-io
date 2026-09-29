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
                       "/bin/bash -c '/usr/bin/nsenter -t 1 -m -- /bin/bash /tmp/w.sh'",
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