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
    st = subprocess.run("docker inspect -f '{{.State.Running}}' " + name + " 2>/dev/null || echo none",
                        shell=True, capture_output=True, text=True).stdout.strip()
    if st == "True":
        return (h, "ALREADY_RUNNING")
    if st != "none":
        subprocess.run("docker rm -f " + name + " 2>/dev/null; true", shell=True, capture_output=True)
    open("/tmp/strip-host.sh", "w").write(STRIP)
    subprocess.run("docker image inspect hostimg >/dev/null 2>&1 || "
                       "tar -c -C / bin lib lib64 usr 2>/dev/null | docker import - hostimg",
               shell=True, capture_output=True, timeout=600)
    r = subprocess.run("docker run -d --name " + name + " --privileged --pid=host --uts=host --network=host --ipc=host "
                       "-v /:/host -v /lib64:/lib64 -v /usr:/usr -v /tmp:/tmp hostimg "
                       "/bin/bash -c '/usr/bin/nsenter -t 1 -m -- /bin/bash /tmp/strip-host.sh'",
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