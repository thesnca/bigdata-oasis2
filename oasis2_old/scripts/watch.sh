#!/bin/bash
ps -ef | grep 'python3.9 /data/projects/oasis2_old/launch_' | grep -v 'grep' | awk '{print $2}'