from . import SimpleMonitoringProcess
import pandas as pd
import json


def to_percentage(x):
    return round(float(x) / 100.0, 2)


maximum_samples = 1000000


class MpStat(SimpleMonitoringProcess):
    def __init__(self):
        super().__init__(
            'mpstat',
            ["mpstat", "-P", "ALL", "-o", "JSON", "1", str(maximum_samples)],
        )

    def load_dataframe(self) -> pd.DataFrame:
        with open(self.stdout_file) as f:
            json_output = json.load(f)

        stats = json_output['sysstat']['hosts'][0]['statistics']
        rows = []
        for records in stats:
            datetime = pd.to_datetime(records['timestamp'])
            for record in records['cpu-load']:
                rows.append({
                    "datetime": datetime,
                    **record
                })

        return pd.DataFrame(rows)
