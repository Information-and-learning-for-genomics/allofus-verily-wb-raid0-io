import subprocess
JOB = '''
import socket, subprocess, os
def probe():
    h = socket.gethostname().split(".")[0]
    def sh(c):
        r = subprocess.run(c, shell=True, capture_output=True, text=True, timeout=30)
        return ((r.stdout or "") + (r.stderr or ""))[-1200:]
    log = open("/tmp/prep-v8.log").read()[-1800:] if os.path.exists("/tmp/prep-v8.log") else "MISSING prep-v8.log (script never ran)"
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