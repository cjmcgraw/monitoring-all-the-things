import datetime as dt
import logging as log
from typing import List, IO
import pandas as pd
import uuid
import os
import traceback

from .processes import MonitoringProcess
from .processes import MonitoringProcess
from .processes.vmstat import VmStat
from .processes.iostat import IoStat
from .processes.strace import Strace
from .processes.mpstat import MpStat
from .processes.nethogs import NetHogs
from .processes.gpu import NvidiaSmi, NvidiaSmiDmon, NvidiaSmiPmon


class MonitoringSession:
    def __init__(self, summary_dir: str):
        self._summary_dir: str = os.path.abspath(summary_dir.rstrip("/"))
        self._raw_process_dir: str = f"{self._summary_dir}/raw"
        self._start: str = None
        self._run: str = None
        self._processes: List[MonitoringProcess] = []
        self._summary_file_handler = None

    def __enter__(self):
        self._start = dt.datetime.now().isoformat()
        self._run = uuid.uuid4().hex
        self._processes = []
        os.mkdir(self._summary_dir)
        os.mkdir(self._raw_process_dir)
        return self

    def start_process(self, process: MonitoringProcess):
        process.start(self._raw_process_dir)
        self._processes.append(process)

    def wait_until_finished(self, ):
        print("awaiting monitoring loop to finish...")
        print("ctrl+c to end monitoring session")

        while True:
            for process in self._processes:
                process.wait(5)

    def __exit__(self, _type, value, tb):
        if value:
            log.error(value)
        if tb:
            traceback.print_exc()
            traceback.print_tb(tb)
        exceptions = []
        end = dt.datetime.now().isoformat()
        dfs = []
        for process in self._processes:
            try:
                process.stop()
            except Exception as err:
                log.error("exception ocurred in process stopping!")
                log.error(err)
                exceptions.append(err)

        print(f"summarizing into {self._summary_dir}")
        for process in self._processes:
            df = (
                process
                .load_dataframe()
                .assign(
                    start=pd.to_datetime(self._start),
                    end=pd.to_datetime(end),
                    run_id=self._run
                )
            )
            filepath = f"{self._summary_dir}/{process.name}.csv"
            log.debug(f"writing {process.name} to {filepath}")
            df.to_csv(filepath)
        log.info("finished summary")

        if exceptions:
            raise Exception(exceptions)
        return True
