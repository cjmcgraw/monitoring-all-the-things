import datetime as dt
import subprocess as sp
import pandas as pd
import contextlib
import argparse
import json
import uuid


def start_command(command, filename):
    ...


class MonitoringSession():

    def __init__(self, summary_file):
        self.start=dt.datetime.utcnow().isoformat()
        self.run = uuid.uuid4().hex
        self.summary_file = summary_file
        self.sub_files = dict()
        self.processes = dict()

    def start_memory(self):
        ...

    def start_cpu(self):
        ...

    def start_io(self):
        ...

    def start_gpu(self):
        ...

    def wait_until_finished(self, ):
        print("awaiting monitoring loop to finish...")
        print("ctrl+c to end monitoring session")

        for name, process in self.processes.items():
            try:
                process.wait(5)
            except sp.TimeoutExpired as err:
                ...
            if process.returncode and process.returncode > 0:
                process.check_returncode()

    def __enter__(self):
        ...

    def __exit__(self, _type, value, traceback):
        end = dt.datetime.utcnow().isoformat()
        print(f"summarizing into {self.summary_file}")
        dfs = dict()
        for name, filepath in sub_files.items():
            df = pd.read_csv(
                filepath, 
                header=0,
                parse_dates=True,
                index_col=0,
            )
            df.assign(
                start=self.start,
                end=end,
                run=self.run
            )
            dfs[name] = df




if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument("--output-file", default=f"{dt.datetime.utcnow().isformat}-monitor.csv")

    args = p.parse_args()

    with MonitoringSession(args.output_file) as monitor:
        monitor.start_memory()
        monitor.start_cpu()
        monitor.start_io()
        monitor.start_gpu()
        monitor.wait_until_finished()

