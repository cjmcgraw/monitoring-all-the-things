from abc import ABC, abstractmethod
import datetime as dt
import logging as log
import subprocess
from typing import List, IO
import pandas as pd
import uuid


class MonitoringProcess(ABC):

    def __init__(self, cmd: List[str], output_file: str):
        self.cmd = cmd
        self.process: subprocess.Popen = None
        self.stdout_file = output_file
        self.stdout_fh: IO[str] = None
        self.stderr_buffer: IO = None
        self.pid: int = None

    def start(self):
        self.stdout_fh: IO[str] = open(self.stdout_file, 'a')
        log.info(f"starting call: {self.cmd} with stdout to {self.stdout_file}")
        self.process = subprocess.Popen(
            self.cmd,
            text=True,
            universal_newlines=True,
            stdout=self.stdout_fh,
            stderr=subprocess.PIPE
        );
        self.pid = self.process.pid
        log.debug(f"started process {self.cmd} with pid: {self.pid}")
        self.stderr_buffer = self.process.stderr

    def stop(self):
        log.info(f"stopping process {self.cmd}")
        self.process.terminate()
        self.process.wait()
        log.debug(f"pid={self.pid}: finished stopping process {self.cmd}")

    def wait(self, timeout):
        try:
            log.debug(f"pid={self.pid}: checking process")
            result = self.process.wait(timeout)
            log.debug(f"pid=${self.pid}: got returncode: {result}")
            if result and result > 0:
                raise subprocess.CalledProcessError(
                    result,
                    self.process.args,
                    'buffered-stdout-in-file',
                    ''.join(self.stderr_buffer.readlines())
                )
        except subprocess.TimeoutExpired as err:
            log.debug(f"pid={self.pid}: process still running")
            ...

    @abstractmethod
    def load_dataframe(self):
        ...


class MonitoringSession:

    def __init__(self, summary_file: str):
        self._summary_file: str = summary_file
        self._start: str = None
        self._run: str = None
        self._processes: List[MonitoringProcess] = []
        self._summary_file_handler = None

    def __enter__(self):
        self._start = dt.datetime.utcnow().isoformat()
        self._run = uuid.uuid4().hex
        self._processes = []
        self._summary_file_handler = open(self._summary_file, 'a')
        return self

    def start_process(self, process: MonitoringProcess):
        process.start()
        self._processes.append(process)

    def wait_until_finished(self, ):
        print("awaiting monitoring loop to finish...")
        print("ctrl+c to end monitoring session")

        while True:
            for process in self._processes:
                process.wait(5)

    def __exit__(self, _type, value, traceback):
        exceptions = []
        end = dt.datetime.utcnow().isoformat()
        dfs = []
        for process in self._processes:
            try:
                process.stop()
            except Exception as err:
                log.error("exception ocurred in process stopping!")
                log.error(err)
                exceptions.append(err)
            df = (
                process
                .load_dataframe()
                .assign(
                    start=self._start,
                    end=end,
                    run_id=self._run
                )
            )
            print(df)

        print(f"summarizing into {self._summary_file}")
        self._summary_file_handler.close()
        log.info("finished summary")

        if exceptions:
            raise Exception(exceptions)
