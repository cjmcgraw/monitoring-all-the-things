from . import SimpleMonitoringProcess
import uuid
import pandas as pd
import itertools as i


def to_percentage(x):
    return round(float(x) / 100.0, 2)


columns_with_transforms = [
    ('datetime', pd.to_datetime),
    ('core', int),
    ('user_cpu', to_percentage),
    ('nice', to_percentage),
    ('sys', to_percentage),
    ('iowait', to_percentage),
    ('irq', to_percentage),
    ('soft', to_percentage),
    ('steal', to_percentage),
    ('guest', to_percentage),
    ('gnice', to_percentage),
    ('idle', to_percentage),
]

maximum_samples = 1000000


class MpStat(SimpleMonitoringProcess):
    def __init__(self):
        super().__init__(
            'mpstat',
            ["mpstat", "-P", "ALL", "1", str(maximum_samples)],
        )

    def load_dataframe(self) -> pd.DataFrame:
        stdout = iter(self)

        def process_row(unparsed_row):
            unparsed_row = unparsed_row.strip()
            valid_row = all([
                unparsed_row,
                'all' not in unparsed_row,
                'CPU' not in unparsed_row,
                'Average' not in unparsed_row
            ])
            if valid_row:
                time, *data = unparsed_row.split()
                if len(data) == 13:
                    am_or_pm, *data = data
                    time += am_or_pm
                row = [time] + data
                return [
                    f(x) for (_, f), x in zip(columns_with_transforms, row)
                ]

        return pd.DataFrame(
            data=filter(lambda x: x, map(process_row, stdout)),
            columns=[e[0] for e in columns_with_transforms]
        ).set_index(['core', 'datetime'])