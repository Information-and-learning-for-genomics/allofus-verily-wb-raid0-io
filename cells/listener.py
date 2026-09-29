import threading, http.server, socketserver, time
try:
    LOGS
    print("listener already up; LOGS:", list(LOGS))
except NameError:
    LOGS = {}
    class HH(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            n = int(self.headers.get("Content-Length", 0))
            host = self.path.strip("/") or "unknown"
            LOGS[host] = self.rfile.read(n).decode(errors="replace")
            print(f"\n===== {host} @ {time.strftime('%H:%M:%S')} =====\n...{LOGS[host][-500:]}", flush=True)
            self.send_response(200); self.end_headers()
        def log_message(self, *a): pass
    class SRV(socketserver.ThreadingTCPServer):
        allow_reuse_address = True; daemon_threads = True
    threading.Thread(target=SRV(("0.0.0.0", 18888), HH).serve_forever, daemon=True).start()
    print("listener started fresh")