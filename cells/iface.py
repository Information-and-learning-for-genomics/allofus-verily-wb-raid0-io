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
