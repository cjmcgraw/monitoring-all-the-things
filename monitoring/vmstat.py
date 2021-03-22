from . import MonitoringProcess
import uuid
import pandas as pd


class VmStat(MonitoringProcess):
    def __init__(self):
        self._vmstat_raw_log = f"/tmp/{uuid.uuid4().hex}-vmstat.log"
        super(VmStat, self).__init__(
            ["vmstat", "-t", "-n", "1", "1000000"],
            self._vmstat_raw_log
        )

    def load_dataframe(self):
        to_mb = lambda b: round(int(b) / 1e6, 2)
        from_percent = lambda x: round(int(x) / 100.0, 2)

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

        def process_row(row):
            time, date, *line = reversed(row.split())
            record = line + ["T".join([date, time])]
            return [
                f(x) for (_, f), x
                in zip(columns_with_transforms, record)
            ]

        with open(self._vmstat_raw_log, 'r') as fh:
            # ignore first two lines of vmstat
            fh.readline()
            fh.readline()

            df = (
                pd.DataFrame(
                    data=map(process_row, fh),
                    columns=[e[0] for e in columns_with_transforms], )
                .set_index('datetime')
                .groupby(pd.Grouper(freq='S'))
                .mean()
            )

        return df

