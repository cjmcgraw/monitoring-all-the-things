#! /usr/bin/env python
import datetime as dt
import logging as log
import argparse
import sys

from monitoring import *


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", default=f"./{dt.datetime.now().isoformat()}-monitoring-session")
    p.add_argument("--include-network", action="store_true")
    p.add_argument("--pid", type=int)
    p.add_argument("--nvidia-gpu", action="store_true")
    p.add_argument("--verbose", action="store_true")

    args = p.parse_args()
    output_dir = str(args.output_dir).rstrip('/')

    log.basicConfig(
        level=log.INFO if not args.verbose else log.DEBUG,
        stream=sys.stdout
    )

    with MonitoringSession(args.output_dir) as monitor:
        if args.pid:
            monitor.start_process(Strace(args.pid))
        if args.include_network:
            monitor.start_process(NetHogs())
        if args.nvidia_gpu:
            monitor.start_process(NvidiaSmi())
            monitor.start_process(NvidiaSmiDmon())
            monitor.start_process(NvidiaSmiPmon())

        monitor.start_process(VmStat())
        monitor.start_process(MpStat())
        monitor.start_process(IoStat())
        monitor.wait_until_finished()
