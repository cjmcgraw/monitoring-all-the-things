#! /usr/bin/env python
import datetime as dt
import logging as log
import argparse
import sys

from monitoring import MonitoringSession
from monitoring.vmstat import VmStat


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument("--output-file", default=f"{dt.datetime.utcnow().isoformat()}-monitor.csv")
    p.add_argument("--exclude-vmstat", action="store_true")
    p.add_argument("--verbose", action="store_true")

    args = p.parse_args()

    log.basicConfig(
        level=log.INFO if not args.verbose else log.DEBUG,
        stream=sys.stdout
    )

    with MonitoringSession(args.output_file) as monitor:
        monitor.start_process(VmStat())
        monitor.wait_until_finished()
