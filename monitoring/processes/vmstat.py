from . import SimpleMonitoringProcess
import pandas as pd


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
        def process_row(row):
            *line, date, time = row.split()
            record = line + ["T".join([date, time])]
            return [
                f(x) for (_, f), x
                in zip(columns_with_transforms, record)
            ]

        stdout = iter(self)

        # ignore first two lines of vmstat
        next(stdout)
        next(stdout)

        df = (
            pd.DataFrame(
                data=map(process_row, stdout),
                columns=[e[0] for e in columns_with_transforms], )
            .set_index('datetime')
            .groupby(pd.Grouper(freq='S'))
            .mean()
        )

        return df

