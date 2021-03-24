import uuid
import pandas as pd
import datetime as dt
import logging as log

from . import MonitoringProcess, _SubMonitoringProcess


class Strace(MonitoringProcess):

    def __init__(self, pid: int):
        output_file = f"/tmp/{uuid.uuid4().hex}-strace.log"
        self._trace_pid = pid
        self._strace_output_file = None
        self._sub_process = _SubMonitoringProcess(
            f"pid-{pid}-strace-stdout",
            ["strace", "-tttTvxC", "-p", f"{pid}"],
        )

    @property
    def name(self):
        return f"strace-{self._trace_pid}"

    def start(self, stdout_dir: str, **kwargs):
        self._strace_output_file = f"{stdout_dir}/{dt.datetime.now().isoformat()}-strace-{self._trace_pid}.log"
        self._sub_process.cmd += [f'-o', self._strace_output_file]
        self._sub_process.start(stdout_dir)

    def stop(self):
        return self._sub_process.stop()

    def wait(self, timeout):
        return self._sub_process.wait(timeout)

    def __iter__(self):
        fh = open(self._strace_output_file)
        try:
            yield from (l.strip() for l in fh)
        finally:
            fh.close()

    def load_dataframe(self) -> pd.DataFrame:
        stdout = list(iter(self))
        def process_stdout():
            for i, line in enumerate(stdout, start=1):
                try:
                    timestamp, *middle, timing = row.split()

                    *syscall, return_code_with_error_msg_maybe = ' '.join(middle).split('=')

                    return_code, *error_msg = return_code_with_error_msg_maybe.split()
                    error_msg = ' '.join(error_msg)

                    fn, *args = '='.join(syscall).split('(')
                    args = '('.join(args)

                    yield {
                        "datetime": dt.datetime.fromtimestamp(float(timestamp)),
                        "timing": float(timing.strip("<").strip(">")),
                        "fn": fn,
                        "args": args,
                        "return_code": int(return_code),
                        "error_msg": error_msg,
                    }
                except Exception as err:
                    log.error(f"{self.name}: failed to process line #{i}")
                    log.error(f"{self.name}: line = {line}")
                    log.exception(err)

        return (
            pd.DataFrame(process_stdout())
            .set_index('datetime')
        )


