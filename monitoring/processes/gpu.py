import pandas as pd
import logging as log

from . import SimpleMonitoringProcess, MonitoringProcess

dmon_columns_with_transformations = [
    ('datetime', pd.to_datetime),
    ('gpu', int),
    ('power_watt', int),
    ('gtemp', int),
    ('mtemp', int),
    ('shared_memory_core_utilization', float),
    ('memory_utilization', float),
    ('encoder_utilization', float),
    ('decoder_utilization', float),
    ('memory_clock', int),
    ('process_clock', int),
    ('power_violation', float),
    ('thermaal_violation', float),
    ('FB_memory', float),
    ('Bar1_memory', float),
    ('single_bit_errors', int),
    ('double_bit_errors', int),
    ('pcie_errors', int),
    ('pcie_read_throughput_mb/s', float),
    ('pcie_write_throughput_mb/s', float)
]


class NvidiaSmiDmon(SimpleMonitoringProcess):

    def __init__(self):
        super().__init__(
            "nvidia-smi-dmon",
            ["nvidia-smi", "dmon", "-d", "1", "-o", "DT", "-s", "pucvmet"]
        )

    def load_dataframe(self) -> pd.DataFrame:
        _, _, *stdout = list(iter(self))

        def process_stdout():
            for i, row in enumerate(stdout, start=2):
                try:
                    date, time, *cols = row.split()
                    record = [f"{date} {time}"] + cols
                    yield {
                        k: f(x) for (k, f), x
                        in zip(dmon_columns_with_transformations, record)
                    }
                except Exception as err:
                    log.error(f"{self.name}: failed to process row #{i}")
                    log.error(f"{self.name}: line = {row}")
                    log.exception(err)

        return (
            pd.DataFrame(process_stdout())
            .set_index('datetime')
        )


def process_int_or_empty(i):
    try:
        return int(i)
    except:
        return None


def process_float_or_empty(f):
    try:
        return float(f)
    except:
        return None


pmon_columns_with_transformations = [
    ("datetime", pd.to_datetime),
    ('gpu', int),
    ('pid', process_int_or_empty),
    ('type', str),
    ('shared_memory_cores_utilization', process_int_or_empty),
    ('memory_utilization', process_int_or_empty),
    ('encoder_utilization', process_int_or_empty),
    ('decoder_utilization', process_int_or_empty),
    ('FB_memory_usage', process_float_or_empty),
    ('command', str)
]


class NvidiaSmiPmon(SimpleMonitoringProcess):

    def __init__(self):
        super().__init__(
            "nvidia-smi-pmon",
            ["nvidia-smi", "pmon", "-d", "1", "-o", "DT", "-s", "um"]
        )

    def load_dataframe(self) -> pd.DataFrame:
        _, _, *stdout = list(iter(self))

        def process_stdout():
            for i, row in enumerate(stdout, start=2):
                try
                    date, time, *cols = row.split()
                    record = [f"{date} {time}"] + cols
                    yield {
                        k: f(x) for (k, f), x
                        in zip(pmon_columns_with_transformations, record)
                    }
                except Exception as err:
                    log.error(f"{self.name}: failed to process record #{i}")
                    log.error(f"{self.name}: line = {row}")
                    log.exception(err)

        return (
            pd.DataFrame(process_stdout())
            .set_index('datetime')
        )


smi_query_columns_with_transformations = [
    ('datetime', pd.to_datetime),
    ('gpu', int),
    ("pstate", str),
    ("buffer_size", int),
    ("total_memory", float),
    ('free_memory', float),
    ('used_memory', float),
    ('compute_mode', str),
    ('gpu_utilization', float),
    ('memory_utilization', float),
    ('encoder_sessions', int),
    ('encoder_average_fps', int),
    ('encoder_average_latency', int),
    ('graphic_clocks', float),
    ('sm_clocks', float),
    ('memory_clocks', float),
    ('video_clocks', float)
]


class NvidiaSmi(SimpleMonitoringProcess):

    def __init__(self):
        super(NvidiaSmi, self).__init__(
            "nvidia-smi",
            [
                "nvidia-smi",
                "--query-gpu=timestamp,index,pstate,accounting.buffer_size,memory.total,memory.free,memory.used,compute_mode,utilization.gpu,utilization.memory,encoder.stats.sessionCount,encoder.stats.averageFps,encoder.stats.averageLatency,clocks.gr,clocks.sm,clocks.mem,clocks.video",
                "--format=csv,noheader,nounits",
                "-l", "1"
            ]
        )

    def load_dataframe(self) -> pd.DataFrame:
        stdout = list(iter(self))

        def process_stdout():
            for i, row in enumerate(stdout, start=1):
                try:
                    record = row.split(',')
                    return {
                        k: f(x)
                        for (k, f), x in zip(smi_query_columns_with_transformations, record)
                    }
                except Exception as err:
                    log.exception(err)
                    log.error(f"{self.name}: failed to process row #{i}")
                    log.error(f"{self.name}: line = {row}")

        return (
            pd.DataFrame(process_stdout())
            .set_index('datetime')
        )
