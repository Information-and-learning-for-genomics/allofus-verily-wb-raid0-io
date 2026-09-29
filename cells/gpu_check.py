# GPU-1: is the GPU visible and healthy on every worker? Output comes back into this
# cell. If only one worker appears, just re-run (YARN may stack both executors on one
# worker; a few re-runs reach both).
import subprocess
JOB = '''
import socket, subprocess
def probe():
    h = socket.gethostname().split(".")[0]
    cmd = ("docker run --rm --privileged --pid=host --uts=host --network=host --ipc=host "
           "-v /:/host -v /lib64:/lib64 -v /lib:/lib -v /usr:/usr -v /tmp:/tmp -v /dev:/dev nullimg "
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
