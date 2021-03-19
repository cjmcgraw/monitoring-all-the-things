#! /usr/bin/env bash
function usage() {
    echo "$0 args"
    echo "This script is a combination of monitoring solutions to monitor"
    echo "memory usage in the system"
    echo ""
    echo "  required arguments:"
    echo "    --output-file   the temporary file to output to"
    echo ""
}
while [ $# ];do case $1 in
    --output-file) output_file="$2" ;;
    --help) usage; exit 0 ;;
    *) break ;;
esac; shift; shift; done

if [[ -z "$output_file" ]] ; then
    echo "missing required arguments"
    usage
    exit 1
fi

set -eu
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT
echo "starting vmstat process monitoring at ${output_file}"
if [[ ! -f "${output_file}" ]]; then
    echo "creating file with first column"
    echo "datetime,procs_waiting,procs_sleeping,virtual_memory_used,free_memory,buffered_memory,cached_memory,swap_in,swap_out,disk_io_in,disk_io_out,system_interrupts,system_context_switches,cpu_user,cpu_sys,cpu_idle,cpu_wait,cpu_stolen" > "${output_file}"
fi

vmstat -n 1 100000 \
    | ts "%Y-%m-%dT%H:%M:%.S" \
    | sed -u "1,2d;s/^[[:space:]]\+//;s/[[:space:]]\+$//;s/[[:space:]]\+/,/g" \
    >> "${output_file}"
