import subprocess, json
r = subprocess.run(["sudo","curl","-s","-m10","http://localhost:8088/ws/v1/cluster/apps"], capture_output=True, text=True, timeout=30)
for a in json.loads(r.stdout)["apps"]["app"]:
    if a["state"] in ("ACCEPTED", "RUNNING"):
        k = subprocess.run(f"sudo -u yarn yarn application -kill {a['id']}", shell=True, capture_output=True, text=True, timeout=60)
        print("KILLED", a["id"], "->", (k.stdout or k.stderr).strip()[-80:], flush=True)
    else:
        print(a["id"], a["state"], a["finalStatus"])