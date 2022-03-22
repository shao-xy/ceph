#!/bin/bash -e

TARGET_BUILD_DIR=build/

set -o pipefail

cd $(dirname $0)/

set -x

function single_line()
{
	set +x
	width=$(tput cols)
	while read line; do
		if test ${#line} -gt ${width}; then
			line="${line:0:$[$width-3]}..."
		fi
		awk '{printf "\033[0K%s\r", $0}' <<< "${line}"
	done
	echo
	set -x
}

function clear_log()
{
	true | sudo tee /var/log/ceph/ceph-mds.*.log
}

ok_flag=0
status_lock=0
function get_status()
{
	if test $status_lock -eq 0; then
		status_lock=1
		stat=$(ceph health detail)
		if [ "$stat" == "HEALTH_OK" ]; then
			ok_flag=1
		fi
		status_lock=0
	fi
}

function wait_ok()
{
	set +x
	echo "Waiting for Ceph cluster to be OK.."
	timecount=0
	while true; do
		get_status
		if test $ok_flag -eq 1; then
			echo
			echo "Waiting finish. Cluster is healthy now."
			break
		fi
		sleep 1
		timecount=$[timecount+1]
		echo -en "\rWaiting ...  $timecount sec"
	done
}

if ! test -d "$TARGET_BUILD_DIR"; then
	./do_cmake.sh
fi

cd $TARGET_BUILD_DIR
make -j32 ceph-mds | single_line
sudo install -p bin/ceph-mds /usr/bin/

clear_log

sudo systemctl daemon-reload
sudo systemctl restart ceph-mds.target

wait_ok
