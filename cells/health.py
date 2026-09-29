import subprocess, json
r = subprocess.run(["sudo","curl","-s","-m10","http://localhost:8088/ws/v1/cluster/nodes"], capture_output=True, text=True, timeout=30)
for x in json.loads(r.stdout)["nodes"]["node"]:
    print("NODE:", x["nodeHostName"].split(".")[0], "|", x["state"], "|", x["healthReport"][:100] or "OK")