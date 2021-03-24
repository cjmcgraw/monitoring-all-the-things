from . import SimpleMonitoringProcess, get_message_error_reading_line
import pandas as pd
import logging as log


def to_mb(b):
    return round(int(b) / 1e6, 2)


def from_percent(x):
    return round(int(x) / 100.0, 2)


columns_with_transforms = [
    ("processes_waiting", int),
    ("processes_sleeping", int),
    ("virtual_memory", to_mb),
    ("free_memory", to_mb),
    ("buffered_memory", to_mb),
    ("cached_memory", to_mb),
    ("swap_in", to_mb),
    ("swap_out", to_mb),
    ("io_bytes_in", to_mb),
    ("io_bytes_out", to_mb),
    ("system_interrupts", int),
    ("context_switches", int),
    ("user_cpu", from_percent),
    ("sys_cpu", from_percent),
    ("idle_cpu", from_percent),
    ("wait_cpu", from_percent),
    ("stolen_cpu", from_percent),
    ("datetime", pd.to_datetime),
]


class VmStat(SimpleMonitoringProcess):

    def __init__(self):
        super().__init__(
            "vmstat",
            ["vmstat", "-t", "-n", "1", "1000000"]
        )

    def load_dataframe(self) -> pd.DataFrame:
        _, _, *stdout = list(iter(self))

        def process_stdout():
            for line_number, line in enumerate(stdout):
                try:
                    *cols, date, time = line.split()
                    record = cols + ["T".join([date, time])]
                    yield {
                        k: f(x) for (k, f), x
                        in zip(columns_with_transforms, record)
                    }
                except Exception as err:
                    log.error(get_message_error_reading_line(
                        self.name,
                        self.stdout_file,
                        stdout,
                        line_number,
                        header_lines_consumed=2
                    ))
                    log.exception(err)

        return pd.DataFrame(process_stdout())

