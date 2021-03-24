from typing import IO
import logging as log
import datetime as dt
import subprocess
import uuid
import pandas as pd
from hashlib import md5

from . import MonitoringProcess, _SubMonitoringProcess, get_message_error_reading_line

columns_with_transforms = [
    ('datetime', pd.to_datetime),
    ("process", str),
    ("device", str),
    ("sent", float),
    ("received", float)
]

maximum_samples = 1000000


class NetHogs(MonitoringProcess):
    def __init__(self):
        self._nethogs_process = _SubMonitoringProcess(
            'nethogs',
            ['nethogs', '-a', '-t', '-d', '1']
        )
        self._timestamp_process = _SubMonitoringProcess(
            'nethogs-with-timestamp',
            ['ts', '%Y-%m-%dT%H:%M:%.S']
        )

    @property
    def name(self):
        return "nethogs"

    def start(self, summary_dir: str, **kwargs):
        self._nethogs_process.start(
            "/tmp",
            stdout=subprocess.PIPE,
            **kwargs
        )

        self._timestamp_process.start(
            summary_dir,
            stdin=self._nethogs_process.process.stdout
        )

    def wait(self, timeout):
        self._nethogs_process.wait(timeout // 2)
        self._timestamp_process.wait(timeout // 2)

    def stop(self):
        self._nethogs_process.stop()
        self._timestamp_process.stop()

    def __iter__(self):
        return iter(self._timestamp_process)

    def load_dataframe(self) -> pd.DataFrame:
        stdout = iter(self)

        lines_skipped = 0
        has_hit_refreshing = False
        while not has_hit_refreshing:
            # need to advance the stdout until we see what we care about
            has_hit_refreshing = 'Refreshing:' in next(stdout)
            lines_skipped += 1

        lines = list(stdout)
        parsed_records = []
        for line_number, line in enumerate(lines):
            unparsed_record = line.split()
            if len(unparsed_record) >= 4:
                try:
                    timestamp, *process, sent, recv = unparsed_record
                    parsed_records.append({
                        "datetime": pd.to_datetime(timestamp),
                        "process_hash": md5(' '.join(process).encode()).hexdigest(),
                        "process": ' '.join(process),
                        'net_kb_sent': round(float(sent), 5),
                        'net_kb_recieved': round(float(recv), 5)
                    })
                except Exception as err:
                    log.error(get_message_error_reading_line(
                        self.name,
                        self._timestamp_process.stdout_file,
                        stdout,
                        line_number,
                        header_lines_consumed=lines_skipped
                    ))
                    log.exception(err)

        return pd.DataFrame(parsed_records)
