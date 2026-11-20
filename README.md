# Server Monitor

A lightweight dashboard for a fleet of GPU servers reachable over SSH: CPU / RAM / GPU usage,
who is using each GPU, and which processes are running — refreshed continuously.
Python standard library only (no Streamlit, Ansible or pandas). UI text is Korean.

- Server table grouped by place tabs, with owner, CPU % (5-min avg), load / cores, RAM,
  one cell per GPU (colour = util, fill = memory), free GPUs, idle time, GPU memory / util, OS and CUDA.
- Click a server row → per-GPU table, GPU users, CPU-heavy users, OS / kernel / driver / CUDA toolkits.
  Click a user → their GPU processes (PID, command line, elapsed time, memory, SM %).
- Servers that stop responding are greyed out with "last collected N min ago" instead of showing stale numbers.

## Quick start

```bash
git clone https://github.com/hyunW3/dashboard_public.git && cd dashboard_public
cp server_info.example.txt server_info.txt   # optional: owners / place tabs (git-ignored)
./run.sh                                     # → http://127.0.0.1:8501
```

Options (`./run.sh <args>` passes them to `collector.py`):

| flag | default | |
|---|---|---|
| `--ssh-config PATH` | `~/.ssh/config` | every `Host` in it is monitored (wildcards skipped) |
| `--hosts a,b` | all | monitor only these aliases |
| `--exclude a,b` | none | skip these aliases |
| `--server-info PATH` | `./server_info.txt` | owners / places, re-read on every refresh |
| `--interval S` | 15 | poll period (seconds) |
| `--window S` | 300 | averaging window (seconds) |
| `--sysinfo-days D` | 7 | OS / CUDA refresh period, cached in `cache/sysinfo.json` |
| `--bind / --port` | `127.0.0.1` / 8501 | web server address |

Stop with `kill $(cat collector.pid)`. To view from a laptop: `ssh -L 8501:127.0.0.1:8501 <collector host>`.
Binding to `0.0.0.0` exposes other users' command lines — keep it on a trusted network.

## Requirements

- Python 3.6+ on the collector host; key-based, non-interactive SSH (`BatchMode`) to every server.
- `nvidia-smi` on GPU servers. Nothing is installed or written on the remote side.

## How it works

| what | how often | how |
|---|---|---|
| CPU %, load, RAM, GPU mem/util, GPU processes | every 15 s | one SSH call per host (ControlMaster reused): `/proc/stat`, `/proc/meminfo`, `nvidia-smi`, `nvidia-smi pmon`, `ps` |
| displayed values | — | 5-min rolling average kept in memory |
| OS, kernel, driver, CUDA toolkits (`/usr/local/cuda-X.Y`) | every 7 days | cached on disk, survives restarts |
| last time each GPU was busy | every 15 s (written at most every 60 s, and immediately when a GPU goes idle) | `cache/gpu_last_used.json`, one timestamp per GPU, overwritten (not a growing log) |

- Aliases pointing at the same HostName/port are merged (your own-user alias preferred); one machine reached via
  two aliases (same remote hostname) is shown once.
- Host-key checking stays on: a server whose key changed shows as unreachable until you verify it
  (`ssh-keygen -R '[host]:port'`).
- **Per-user GPU util** is an estimate: each GPU's utilisation split across its processes by `pmon` SM % (memory share if unavailable).
- **5-min averages** count missing samples as 0, so jobs that just started look lower.
- **Idle (유휴)** = among the GPUs that are free right now, how long the longest-idle one has been unused; `–` when no GPU
  is free. `≥` means the GPU has not been busy since monitoring began, so the real idle time is at least that long
  (`nvidia-smi` keeps no history from before the collector started).
- **Free GPU** = no processes and < 1 GB used. Processes inside containers whose owner `ps` cannot see show as `?(container)`.

## Files

```
collector.py              collector + web server (/ and /api/status)
index.html                dashboard UI
run.sh                    (re)start in the background (pid in collector.pid)
server_info.example.txt   template for server_info.txt (owners / places) — real file is git-ignored
```
