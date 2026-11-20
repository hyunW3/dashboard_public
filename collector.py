#!/usr/bin/env python3
"""Server monitor: collector + web server (Python 3.6+ stdlib only).

Every --interval seconds each host is polled over one SSH call (ControlMaster
reused) for CPU / RAM / GPU / GPU processes; a rolling --window (default 5 min)
is kept in memory. OS / kernel / driver / CUDA toolkits are collected rarely
(--sysinfo-days, default 7) and cached on disk in cache/sysinfo.json.

  /            -> index.html
  /api/status  -> JSON snapshot

Usage:
  python3 collector.py                         # hosts from ~/.ssh/config (minus --exclude)
  python3 collector.py --ssh-config ./ssh_config --port 8501
  python3 collector.py --hosts gpu01,gpu02
"""
import argparse
import json
import os
import re
import subprocess
import threading
import time
from collections import defaultdict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))

POLL_SCRIPT = r"""
export LC_ALL=C
echo '##HOST'; hostname
echo '##CPU'; head -1 /proc/stat
echo '##LOAD'; cat /proc/loadavg; nproc
echo '##MEM'; grep -E '^(MemTotal|MemAvailable):' /proc/meminfo
echo '##TOPCPU'; ps -eo user:32=,pcpu= --sort=-pcpu 2>/dev/null | head -400
if command -v nvidia-smi >/dev/null 2>&1; then
echo '##GPU'; nvidia-smi --query-gpu=index,uuid,name,memory.used,memory.total,utilization.gpu,temperature.gpu,power.draw --format=csv,noheader,nounits
echo '##APPS'; nvidia-smi --query-compute-apps=gpu_uuid,pid,used_memory --format=csv,noheader,nounits
echo '##PMON'; timeout 6 nvidia-smi pmon -c 1 -s u 2>/dev/null
echo '##PS'; pids=$(nvidia-smi --query-compute-apps=pid --format=csv,noheader 2>/dev/null | tr -d ' ' | sort -u | paste -sd, -)
[ -n "$pids" ] && ps -o pid=,user:32=,etimes=,args= -p "$pids" 2>/dev/null | cut -c1-600
fi
echo '##END'
"""

SYSINFO_SCRIPT = r"""
export LC_ALL=C
echo '##OS'; . /etc/os-release 2>/dev/null; echo "${PRETTY_NAME:-unknown}"; echo "${ID}${VERSION_ID}"
echo '##KERNEL'; uname -r
echo '##CUDA'; ls -1 /usr/local 2>/dev/null | grep -E '^cuda-[0-9]+\.[0-9]+$' | sort -V
echo '##DRIVER'; nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null | head -1
nvidia-smi 2>/dev/null | grep -o 'CUDA Version: [0-9.]*' | head -1 | sed 's/CUDA Version: //'
echo '##END'
"""


# ---------------------------------------------------------------- config

def parse_ssh_config(path, exclude):
    """Aliases from an ssh config, deduped by (HostName, Port); prefer own-user aliases."""
    hosts, cur = [], None
    try:
        lines = open(path).read().splitlines()
    except OSError:
        return []
    for l in lines:
        if l.strip().startswith("#"):
            continue
        m = re.match(r"\s*(\S+)\s+(.*)", l)
        if not m:
            continue
        k, v = m.group(1).lower(), m.group(2).strip()
        if k == "host":
            cur = {"alias": v.split()[0]}
            hosts.append(cur)
        elif cur is not None:
            cur[k] = v
    me = os.environ.get("USER", "")
    best, order = {}, []
    for h in hosts:
        a = h["alias"]
        if a in exclude or "*" in a or "?" in a:
            continue
        key = (h.get("hostname", a), h.get("port", "22"))
        pri = 0 if h.get("user") in (None, me) else 1
        if key not in best:
            order.append(key)
        if key not in best or pri < best[key][0]:
            best[key] = (pri, a)
    return [best[k][1] for k in order]


def load_server_info(path):
    """server_info.txt: optional '[place]' headers, then '<ip> <owner text>' lines.
    Re-read on every request so edits apply without restart. -> ({ip: owner}, {ip: place}, [places])"""
    owners, places, order, place = {}, {}, [], None
    try:
        for l in open(path, encoding="utf-8"):
            l = l.strip()
            if not l or l.startswith("#"):
                continue
            m = re.match(r"^\[(.+)\]$", l)
            if m:
                place = m.group(1).strip()
                if place not in order:
                    order.append(place)
                continue
            p = l.split(None, 1)
            owners[p[0]] = p[1].strip() if len(p) > 1 else ""
            if place:
                places[p[0]] = place
    except OSError:
        pass
    return owners, places, order


# ---------------------------------------------------------------- helpers

def split_sections(text):
    sec, cur = {}, None
    for line in text.splitlines():
        if line.startswith("##"):
            cur = line[2:].strip()
            sec[cur] = []
        elif cur is not None:
            sec[cur].append(line)
    return sec


def fnum(x, default=None):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


class Ssh:
    def __init__(self, ssh_config, ctl_dir):
        self.base = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
                     "-o", "ServerAliveInterval=10", "-o", "ControlMaster=auto",
                     "-o", "ControlPath=%s/%%r@%%h:%%p" % ctl_dir, "-o", "ControlPersist=15m"]
        if ssh_config:
            self.base[1:1] = ["-F", ssh_config]
        self.cfg = ssh_config

    def run(self, alias, script, timeout):
        r = subprocess.run(self.base + [alias, "bash -s"], input=script, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, universal_newlines=True, timeout=timeout)
        if "##END" not in r.stdout:
            err = r.stderr.strip().splitlines() or ["exit %d" % r.returncode]
            key = [e for e in err if any(w in e.lower() for w in ("key", "refused", "timed out", "denied"))]
            raise RuntimeError((key or err)[-1][:200])
        return split_sections(r.stdout)

    def resolve_ip(self, alias):
        cmd = ["ssh"] + (["-F", self.cfg] if self.cfg else []) + ["-G", alias]
        try:
            out = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                 universal_newlines=True, timeout=10).stdout
            for l in out.splitlines():
                if l.startswith("hostname "):
                    return l.split(None, 1)[1].strip()
        except Exception:  # noqa
            pass
        return alias


class SysinfoCache:
    """OS/CUDA info per alias, persisted to disk; refreshed every `max_age` seconds."""

    def __init__(self, path, max_age):
        self.path, self.max_age, self.lock = path, max_age, threading.Lock()
        try:
            self.data = json.load(open(path))
        except (OSError, ValueError):
            self.data = {}

    def get(self, alias):
        with self.lock:
            return self.data.get(alias)

    def due(self, alias):
        d = self.get(alias)
        return d is None or time.time() - d.get("ts", 0) > self.max_age

    def put(self, alias, info):
        with self.lock:
            self.data[alias] = info
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            tmp = self.path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=1, sort_keys=True)
            os.replace(tmp, self.path)


class GpuIdleCache:
    """Last time each GPU was busy, per remote hostname, persisted to disk.
    busy = has a compute process or >= 1 GB used (inverse of the 'free GPU' rule).
    'first' = when this host was first monitored (lower bound for never-busy GPUs)."""

    def __init__(self, path):
        self.path, self.lock, self.saved = path, threading.Lock(), 0.0
        try:
            self.data = json.load(open(path))
        except (OSError, ValueError):
            self.data = {}

    def update(self, host, sample):
        now = time.time()
        busy = {pr["gpu"] for pr in sample["procs"]} | {g["idx"] for g in sample["gpus"] if g["mem_used"] >= 1024}
        with self.lock:
            h = self.data.setdefault(host, {"first": now, "last": {}})
            freed = False
            for g in sample["gpus"]:
                k = str(g["idx"])
                if g["idx"] in busy:
                    h["last"][k] = now
                elif h["last"].get(k, 0) > self.saved:
                    freed = True  # busy -> idle since last save: persist right away
            if freed or now - self.saved > 60:
                self._save(now)

    def _save(self, now):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.data, f, indent=1, sort_keys=True)
        os.replace(tmp, self.path)
        self.saved = now

    def idle(self, host, idx, now):
        """-> (idle seconds, is_lower_bound) or (None, False) if unknown."""
        with self.lock:
            h = self.data.get(host)
            if not h:
                return None, False
            t = h["last"].get(str(idx))
            return (now - t, False) if t else (now - h["first"], True)


def parse_sysinfo(sec):
    os_lines = sec.get("OS", []) + ["", ""]
    drv = sec.get("DRIVER", []) + ["", ""]
    return {"ts": time.time(), "os": os_lines[0].strip(), "os_short": os_lines[1].strip(),
            "kernel": (sec.get("KERNEL") or [""])[0].strip(),
            "cuda_toolkits": [c.strip() for c in sec.get("CUDA", []) if c.strip()],
            "driver": drv[0].strip(), "driver_cuda": drv[1].strip()}


# ---------------------------------------------------------------- polling

class HostState:
    def __init__(self, alias, ip, window):
        self.alias, self.ip, self.window = alias, ip, window
        self.samples = deque()
        self.prev_cpu = None
        self.error = None
        self.last_ok = None
        self.hostname = alias
        self.static = {}
        self.lock = threading.Lock()


def parse_sample(st, sec):
    s = {"ts": time.time()}
    st.hostname = ((sec.get("HOST") or [st.alias])[0].strip()) or st.alias

    vals = [int(x) for x in (sec.get("CPU") or [""])[0].split()[1:] if x.isdigit()]
    cpu = None
    if len(vals) >= 4:
        idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
        total = sum(vals[:8])
        if st.prev_cpu and total > st.prev_cpu[0]:
            cpu = max(0.0, min(100.0, 100.0 * (1 - (idle - st.prev_cpu[1]) / (total - st.prev_cpu[0]))))
        st.prev_cpu = (total, idle)
    s["cpu"] = cpu
    load = sec.get("LOAD") or ["", ""]
    lp = load[0].split()
    s["load1"] = fnum(lp[0]) if lp else None
    if len(load) > 1 and load[1].strip().isdigit():
        st.static["ncpu"] = int(load[1])
    mem = {}
    for l in sec.get("MEM", []):
        p = l.split()
        if len(p) >= 2:
            mem[p[0].rstrip(":")] = int(p[1])
    if "MemTotal" in mem:
        st.static["ram_total_gb"] = mem["MemTotal"] / 1048576
        s["ram_used_gb"] = (mem["MemTotal"] - mem.get("MemAvailable", 0)) / 1048576
    ucpu = defaultdict(float)
    for l in sec.get("TOPCPU", []):
        p = l.split()
        if len(p) >= 2:
            ucpu[p[0]] += fnum(p[1], 0.0)
    s["user_cpu"] = {u: v for u, v in ucpu.items() if v >= 50}

    gpus, uuid2idx = [], {}
    for l in sec.get("GPU", []):
        p = [x.strip() for x in l.split(",")]
        if len(p) < 6 or not p[0].isdigit():
            continue
        uuid2idx[p[1]] = int(p[0])
        gpus.append({"idx": int(p[0]), "name": p[2], "mem_used": fnum(p[3], 0.0),
                     "mem_total": fnum(p[4], 0.0), "util": fnum(p[5], 0.0),
                     "temp": fnum(p[6]) if len(p) > 6 else None,
                     "power": fnum(p[7]) if len(p) > 7 else None})
    s["gpus"] = gpus
    if gpus:
        st.static["gpu_name"] = gpus[0]["name"]

    psinfo = {}
    for l in sec.get("PS", []):
        p = l.split(None, 3)
        if len(p) >= 3 and p[0].isdigit():
            psinfo[int(p[0])] = {"user": p[1], "etime": int(p[2]) if p[2].isdigit() else None,
                                 "cmd": p[3] if len(p) > 3 else ""}
    sm = {}
    for l in sec.get("PMON", []):
        p = l.split()
        if not l.startswith("#") and len(p) >= 4 and p[0].isdigit() and p[1].isdigit():
            sm[(int(p[0]), int(p[1]))] = fnum(p[3], 0.0)
    procs = []
    for l in sec.get("APPS", []):
        p = [x.strip() for x in l.split(",")]
        if len(p) < 3 or not p[1].isdigit() or p[0] not in uuid2idx:
            continue
        gi, pid = uuid2idx[p[0]], int(p[1])
        info = psinfo.get(pid, {"user": "?(container)", "etime": None, "cmd": ""})
        procs.append({"gpu": gi, "pid": pid, "mem": fnum(p[2], 0.0), "sm": sm.get((gi, pid)),
                      "user": info["user"], "etime": info["etime"], "cmd": info["cmd"]})
    # split each GPU's util over its processes by pmon SM share (fallback: memory share)
    by_gpu = defaultdict(list)
    for pr in procs:
        by_gpu[pr["gpu"]].append(pr)
    gutil = {g["idx"]: g["util"] for g in gpus}
    for gi, plist in by_gpu.items():
        smsum = sum(pr["sm"] or 0 for pr in plist)
        memsum = sum(pr["mem"] for pr in plist)
        for pr in plist:
            share = ((pr["sm"] or 0) / smsum if smsum > 0 else
                     pr["mem"] / memsum if memsum > 0 else 1.0 / len(plist))
            pr["util"] = gutil.get(gi, 0.0) * share
    s["procs"] = procs
    return s


def poll_host(st, ssh, sysinfo, gpuidle, args):
    while True:
        t0 = time.time()
        try:
            sec = ssh.run(st.alias, POLL_SCRIPT, max(30, args.interval * 2))
            s = parse_sample(st, sec)
            with st.lock:
                st.samples.append(s)
                while st.samples and st.samples[0]["ts"] < time.time() - st.window:
                    st.samples.popleft()
                st.error, st.last_ok = None, time.time()
            gpuidle.update(st.hostname, s)
            if sysinfo.due(st.alias):
                sysinfo.put(st.alias, parse_sysinfo(ssh.run(st.alias, SYSINFO_SCRIPT, 30)))
        except Exception as e:  # noqa
            with st.lock:
                st.error = str(e) or e.__class__.__name__
        time.sleep(max(1.0, args.interval - (time.time() - t0)))


# ---------------------------------------------------------------- summary

def summarize(states, args, sysinfo, gpuidle):
    now = time.time()
    owners, places, place_order = load_server_info(args.server_info)
    # same machine via two aliases (gpu01 / gpu01_admin) -> keep one, prefer one listed in server_info
    chosen = {}
    for st in states:
        if st.samples:
            cur = chosen.get(st.hostname)
            if cur is None or (cur.ip not in owners and st.ip in owners):
                chosen[st.hostname] = st
    servers = []
    for st in states:
        if st.samples and chosen.get(st.hostname) is not st:
            continue
        with st.lock:
            samples = list(st.samples)
            err, last_ok, static, hn = st.error, st.last_ok, dict(st.static), st.hostname
        si = sysinfo.get(st.alias) or {}
        srv = {"alias": st.alias.split("_")[0], "hostname": hn, "ip": st.ip,
               "owner": owners.get(st.ip, ""), "place": places.get(st.ip, "기타"),
               "error": err, "last_ok": last_ok, "age": None if last_ok is None else now - last_ok,
               "ncpu": static.get("ncpu"), "ram_total_gb": static.get("ram_total_gb"),
               "gpu_name": static.get("gpu_name"), "sysinfo": si}
        if not samples:
            srv["status"] = "unreachable"
            servers.append(srv)
            continue
        srv["status"] = "stale" if now - last_ok > 120 else ("warn" if err else "ok")
        n, last = len(samples), samples[-1]
        srv["cpu"] = mean([s["cpu"] for s in samples])
        srv["load1"] = last.get("load1")
        srv["ram_used_gb"] = mean([s.get("ram_used_gb") for s in samples])
        srv["cpu_users"] = sorted(last.get("user_cpu", {}).items(), key=lambda kv: -kv[1])[:5]
        gacc = defaultdict(lambda: {"mem": [], "util": []})
        for s in samples:
            for g in s["gpus"]:
                gacc[g["idx"]]["mem"].append(g["mem_used"])
                gacc[g["idx"]]["util"].append(g["util"])
        gpus = [{"idx": g["idx"], "name": g["name"], "mem_total": g["mem_total"],
                 "mem_avg": mean(gacc[g["idx"]]["mem"]), "util_avg": mean(gacc[g["idx"]]["util"]),
                 "mem_now": g["mem_used"], "util_now": g["util"], "temp": g["temp"],
                 "power": g["power"], "users": []} for g in last["gpus"]]
        gmap = {g["idx"]: g for g in gpus}
        pacc = {}
        for s in samples:
            for pr in s["procs"]:
                a = pacc.setdefault((pr["gpu"], pr["pid"]), {"mem": 0.0, "util": 0.0, "sm": [], "last": pr})
                a["mem"] += pr["mem"]
                a["util"] += pr.get("util", 0.0)
                if pr["sm"] is not None:
                    a["sm"].append(pr["sm"])
                a["last"] = pr
        live = {(pr["gpu"], pr["pid"]) for pr in last["procs"]}
        su = defaultdict(lambda: {"mem": 0.0, "util": 0.0, "gpus": set(), "procs": []})
        for (gi, pid), a in pacc.items():
            pr, alive = a["last"], (gi, pid) in live
            row = {"gpu": gi, "pid": pid, "user": pr["user"], "mem_avg": a["mem"] / n,
                   "util_avg": a["util"] / n, "mem_now": pr["mem"] if alive else 0.0,
                   "sm_avg": mean(a["sm"]), "alive": alive, "etime": pr["etime"], "cmd": pr["cmd"]}
            u = su[pr["user"]]
            u["mem"] += row["mem_avg"]
            u["util"] += row["util_avg"]
            u["procs"].append(row)
            if alive:
                u["gpus"].add(gi)
                if gi in gmap and pr["user"] not in gmap[gi]["users"]:
                    gmap[gi]["users"].append(pr["user"])
        srv["gpus"] = gpus
        idle = []
        for g in gpus:
            g["free"] = not g["users"] and (g["mem_now"] or 0) < 1024
            g["idle_s"], g["idle_lb"] = gpuidle.idle(hn, g["idx"], now) if g["free"] else (None, False)
            if g["idle_s"] is not None:
                idle.append((g["idle_s"], g["idle_lb"]))
        srv["gpu_free"] = sum(1 for g in gpus if g["free"])
        # longest-idle among currently free GPUs
        srv["idle_max"], srv["idle_lb"] = max(idle) if idle else (None, False)
        srv["users"] = sorted([{"user": k, "mem_avg": v["mem"], "util_avg": v["util"], "gpus": sorted(v["gpus"]),
                                "procs": sorted(v["procs"], key=lambda r: (r["gpu"], -r["mem_avg"]))}
                               for k, v in su.items()], key=lambda r: -r["mem_avg"])
        srv["mem_total_all"] = sum(g["mem_total"] for g in gpus)
        srv["mem_avg_all"] = sum(g["mem_avg"] or 0 for g in gpus)
        srv["util_avg_all"] = mean([g["util_avg"] for g in gpus])
        servers.append(srv)
    # failing aliases of a machine that is reachable under another alias -> hide
    up = [x for x in servers if x["status"] != "unreachable"]
    up_alias, up_ip = {x["alias"] for x in up}, {x["ip"] for x in up}
    kept, seen_down = [], set()
    for x in servers:
        if x["status"] == "unreachable":
            if x["alias"] in up_alias or x["ip"] in up_ip or x["alias"] in seen_down:
                continue
            seen_down.add(x["alias"])
        kept.append(x)
    order = place_order + [p for p in sorted({x["place"] for x in kept}) if p not in place_order]
    return {"generated": now, "window": args.window, "interval": args.interval,
            "sysinfo_days": args.sysinfo_days, "places": order, "servers": kept}


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hosts", default="", help="comma list of ssh aliases (default: all in ssh config)")
    ap.add_argument("--exclude", default="", help="comma list of aliases to skip")
    ap.add_argument("--ssh-config", default="", help="ssh config file (default: ~/.ssh/config)")
    ap.add_argument("--server-info", default=os.path.join(HERE, "server_info.txt"))
    ap.add_argument("--interval", type=float, default=15, help="poll interval (s)")
    ap.add_argument("--window", type=float, default=300, help="averaging window (s)")
    ap.add_argument("--sysinfo-days", type=float, default=7, help="OS/CUDA refresh period (days)")
    ap.add_argument("--cache", default=os.path.join(HERE, "cache", "sysinfo.json"))
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8501)
    args = ap.parse_args()

    cfg = args.ssh_config or os.path.expanduser("~/.ssh/config")
    exclude = {x for x in args.exclude.split(",") if x}
    hosts = [h for h in args.hosts.split(",") if h] or parse_ssh_config(cfg, exclude)
    print("hosts (%d): %s" % (len(hosts), " ".join(hosts)), flush=True)
    ctl_dir = os.path.join(os.path.expanduser("~"), ".ssh", "monitor_ctl")
    os.makedirs(ctl_dir, mode=0o700, exist_ok=True)
    ssh = Ssh(args.ssh_config or None, ctl_dir)
    sysinfo = SysinfoCache(args.cache, args.sysinfo_days * 86400)
    gpuidle = GpuIdleCache(os.path.join(os.path.dirname(args.cache), "gpu_last_used.json"))
    states = [HostState(h, ssh.resolve_ip(h), args.window) for h in hosts]
    for st in states:
        threading.Thread(target=poll_host, args=(st, ssh, sysinfo, gpuidle, args), daemon=True).start()

    cache = {"t": 0, "body": b"{}"}
    cache_lock = threading.Lock()

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?")[0]
            if path == "/api/status":
                with cache_lock:
                    if time.time() - cache["t"] > 3:
                        cache["body"] = json.dumps(summarize(states, args, sysinfo, gpuidle), ensure_ascii=False).encode()
                        cache["t"] = time.time()
                    body = cache["body"]
                self._send(200, body, "application/json; charset=utf-8")
            elif path in ("/", "/index.html"):
                with open(os.path.join(HERE, "index.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            else:
                self._send(404, b"not found", "text/plain")

    srv = ThreadingHTTPServer((args.bind, args.port), H)
    print("serving on http://%s:%d" % (args.bind, args.port), flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
