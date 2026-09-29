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
