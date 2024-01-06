#!/bin/bash
nohup /data/projects/python-env/oasis2-venv/bin/python3.9 /data/projects/oasis2/launch_web.py -n kes -p 28081 >/dev/null &
nohup /data/projects/python-env/oasis2-venv/bin/python3.9 /data/projects/oasis2/launch_web.py -n khbase -p 28082 >/dev/null &
nohup /data/projects/python-env/oasis2-venv/bin/python3.9 /data/projects/oasis2/launch_manager.py -n manager01 >/dev/null &
nohup /data/projects/python-env/oasis2-venv/bin/python3.9 /data/projects/oasis2/launch_manager.py -n manager02 >/dev/null &
nohup /data/projects/python-env/oasis2-venv/bin/python3.9 /data/projects/oasis2/launch_worker.py -n worker01 >/dev/null &
nohup /data/projects/python-env/oasis2-venv/bin/python3.9 /data/projects/oasis2/launch_worker.py -n worker02 >/dev/null &
nohup /data/projects/python-env/oasis2-venv/bin/python3.9 /data/projects/oasis2/launch_worker.py -n worker03 >/dev/null &