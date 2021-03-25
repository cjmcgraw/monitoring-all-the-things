from . import SimpleMonitoringProcess
import logging as log
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
        rows = []
        try:
            with open(self.stdout_file) as f:
                json_output = json.load(f)

            stats = json_output['sysstat']['hosts'][0]['statistics']
            for records in stats:
                datetime = pd.to_datetime(records['timestamp'])
                for record in records['cpu-load']:
                    rows.append({
                        "datetime": datetime,
                        **record
                    })
        except Exception as err:
            log.error(f"{self.name}: failed to parse json file at {self.stdout_file}")
            log.exception(err)
            log.error(f"{self.name} related file will be empty")

        return pd.DataFrame(rows)
