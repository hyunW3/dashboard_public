#!/usr/bin/env bash
# (re)start the monitor detached; extra args go to collector.py (e.g. --port 8501 --ssh-config ./ssh_config)
cd "$(dirname "$0")"
[ -f collector.pid ] && kill "$(cat collector.pid)" 2>/dev/null && sleep 1
setsid nohup python3 collector.py "$@" > collector.log 2>&1 < /dev/null &
echo $! > collector.pid
echo "started pid $(cat collector.pid)  log: $PWD/collector.log"
