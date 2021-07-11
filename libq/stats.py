from dataclasses import dataclass, field
import numpy as np
import pandas as pd
import typing as t


def _diff(s: pd.Series):
    return s.diff().fillna(s)


class Metric:
    def __init__(self):
        self.data = []
        self.acc = 0

    def add(self, v):
        self.acc += v

    def set(self, v):
        self.acc = v

    def tick(self):
        self.data.append(self.acc)

    def tick_and_reset(self):
        self.data.append(self.acc)
        self.acc = 0


class Histogram:
    def __init__(self):
        self.data = []
        self.acc = []

    def add(self, v):
        self.acc += v

    def tick(self):
        self.data.append(self.acc)
        self.acc = []


@dataclass
class RunStats:
    n_requests: Metric = field(default_factory=Metric)
    n_serviced: Metric = field(default_factory=Metric)
    n_completed: Metric = field(default_factory=Metric)
    c_pending: Metric = field(default_factory=Metric)
    c_qsize: Metric = field(default_factory=Metric)
    c_util: Metric = field(default_factory=Metric)
    response_time: Histogram = field(default_factory=Histogram)
    service_time: Histogram = field(default_factory=Histogram)

    def tick(self):
        self.n_requests.tick()
        self.n_serviced.tick()
        self.n_completed.tick()
        self.c_pending.tick_and_reset()
        self.c_qsize.tick_and_reset()
        self.c_util.tick_and_reset()
        self.response_time.tick()
        self.service_time.tick()

    def df(self, percentiles: t.List[float] = [100]) -> pd.DataFrame:
        df = pd.DataFrame(
            {
                "requests": self.n_requests.data,
                "serviced": self.n_serviced.data,
                "completed": self.n_completed.data,
                "pending": self.c_pending.data,  # average pending requests
                "qsize": self.c_qsize.data,  # average queue size
                "utilization": self.c_util.data,  # average utilization
            }
        )
        df["r_request"] = _diff(df.requests)
        df["r_serviced"] = _diff(df.serviced)
        df["r_completed"] = _diff(df.completed)
        rts = [np.array(times) for times in self.response_time.data]
        sts = [np.array(times) for times in self.service_time.data]
        for p in percentiles:
            df["response_time_p{:g}".format(p)] = [
                (np.percentile(ar, p) if len(ar) > 0 else None) for ar in rts
            ]
            df["service_time_p{:g}".format(p)] = [
                (np.percentile(ar, p) if len(ar) > 0 else None) for ar in sts
            ]
        return df

    def response_times(self):
        return np.concatenate(self.response_time.data)

    def service_times(self):
        return np.concatenate(self.service_time.data)
