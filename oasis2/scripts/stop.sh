#!/bin/bash
ps -ef | grep 'python3.9 /data/projects/oasis2/launch_' | grep -v 'grep' | awk '{print $2}' | xargs kill
sleep 5