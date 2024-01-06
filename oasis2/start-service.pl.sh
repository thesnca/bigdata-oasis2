#!/bin/bash
# generated-by-penglai
# generate time: 2023-06-01 11:56:45
set -ex
echo "-------------- Start date: $(date) ------------------"
WORK_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "Entry work directory: $WORK_DIR"
if [[ "$WORK_DIR" != "/data/projects/oasis2" ]];then
echo "Current directory not match: /data/projects/oasis2. Can't start service."
exit 1
fi
cd "$WORK_DIR"
echo "Start exec start command."
source /etc/profile && set -m && export OASIS_REGION=galaxy;export OASIS_ENV=PROD && cd scripts;sh restart.sh
echo "-------------- Complete ------------------"
