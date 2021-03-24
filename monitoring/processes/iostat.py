from . import SimpleMonitoringProcess
import uuid
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
        with open(self.stdout_file) as f:
            data = json.load(f)

        records = []
        for record in data['sysstat']['hosts'][0]['statistics']:
            datetime = pd.to_datetime(record['timestamp'])
            for value in record['disk']:
                records.append(dict(**value, datetime=datetime))

        return pd.DataFrame(data=records).set_index(['datetime', 'disk_device'])
