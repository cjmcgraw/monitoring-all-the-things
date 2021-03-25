from . import SimpleMonitoringProcess
import logging as log
import pandas as pd
import json

maximum_samples = 1000000


class IoStat(SimpleMonitoringProcess):
    def __init__(self):
        super().__init__(
            'iostat',
            ["iostat", "-mxt", "-o", "JSON", "1", str(maximum_samples)]
        )

    def load_dataframe(self) -> pd.DataFrame:
        records = []
        try:
            with open(self.stdout_file) as f:
                data = json.load(f)

            for record in data['sysstat']['hosts'][0]['statistics']:
                datetime = pd.to_datetime(record['timestamp'])
                for value in record['disk']:
                    records.append(dict(**value, datetime=datetime))
        except Exception as err:
            log.error(f"{self.name}: failed to process json at {self.stdout_file}")
            log.exception(err)
            log.error(f"{self.name} associated files will most likely be empty")

        return pd.DataFrame(records)
