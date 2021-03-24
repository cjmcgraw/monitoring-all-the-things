from abc import ABC, abstractmethod
import datetime as dt
from typing import IO, List, Dict, Iterable, Callable, Generator
import logging as log
import pandas as pd
import subprocess
from textwrap import dedent
import os



class MonitoringProcess(ABC):

    @property
    @abstractmethod
    def name(self): ...

    @abstractmethod
    def start(self, stdout_dir: str, **kwargs): ...

    @abstractmethod
    def stop(self): ...

    @abstractmethod
    def wait(self, timeout): ...

    @abstractmethod
    def __iter__(self): ...

    @abstractmethod
    def load_dataframe(self) -> pd.DataFrame: ...


class SimpleMonitoringProcess(MonitoringProcess):

    def __init__(self, name: str, cmd: List[str]):
        self._name = name
        self.cmd = cmd
        self.process: subprocess.Popen = None
        self.stdout_file = None
        self.stdout_fh: IO[str] = None
        self.stderr_buffer: IO = None
        self.pid: int = None

    @property
    def name(self):
        return self._name

    def start(self, stdout_dir: str, **kwargs):
        self.stdout_file = f"{stdout_dir.rstrip('/')}/{dt.datetime.now().isoformat()}-{self.name}.log"
        self.stdout_fh: IO[str] = open(self.stdout_file, 'w')

        log.debug(f"starting call: {self.cmd}")
        kwargs.setdefault("stdout", self.stdout_fh)
        kwargs.setdefault("stderr", subprocess.PIPE)
        kwargs.setdefault("universal_newlines", True)
        kwargs.setdefault("text", True)
        self.process = subprocess.Popen(self.cmd, **kwargs)

        self.pid = self.process.pid
        log.info(f"pid={self.pid}: started {self.cmd} to {self.stdout_file}")
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
            log.debug(f"pid={self.pid}: got returncode: {result}")
            if result and result > 0:
                log.error(f"pid={self.pid}: process failed!")
                stderr = ''.join(self.stderr_buffer.readlines())
                log.error(stderr)
                raise subprocess.CalledProcessError(
                    result,
                    self.process.args,
                    'buffered-stdout-in-file',
                    stderr
                )
        except subprocess.TimeoutExpired as err:
            log.debug(f"pid={self.pid}: process still running")
            ...

    def __iter__(self):
        fh = open(self.stdout_file)
        try:
            yield from (l.strip() for l in fh)
        finally:
            fh.close()


class _SubMonitoringProcess(SimpleMonitoringProcess):
    def load_dataframe(self) -> pd.DataFrame:
        raise NotImplementedError("Must implement from abstract base class!!")


def get_message_error_reading_line(process_name, filepath, lines, line_number, header_lines_consumed=0):
    actual_line_number = line_number + header_lines_consumed + 1
    i = line_number
    msg = dedent(f"""
    {process_name}: Error reading line from stdout
    
    process: {process_name}
    file: {filepath}
    line number: {line_number + 1}
    lines:
    
    """)
    first_section = lines[max(0, i - 5): i - 1]
    if first_section:
        for ln, section in enumerate(first_section, start=actual_line_number - len(first_section)):
            msg += f"   {ln} | {section} \n"

    msg += f">  {actual_line_number} | {lines[i]}\n"

    last_section = lines[i: i + 5]
    if last_section:
        for ln, section in enumerate(last_section, start=actual_line_number + 1):
            msg += f"   {ln} | {section} \n"

    return msg



