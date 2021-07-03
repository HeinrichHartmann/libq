import numpy as np
import typing as t
from dataclasses import dataclass
import pandas as pd

# from pydantic.dataclasses import dataclass


class Clock:
    def __init__(self):
        self._t = 0

    def tick(self):
        self._t += 1

    def t(self):  # get time
        return self._t


class Request:
    def __init__(self, clock: Clock):
        self.t_start: int = 0
        self.t_end: int = 0
        self.t_service: int = 0
        self.status: t.Optional[bool] = None
        self.clock: Clock = clock

    def start(self):
        self.t_start = self.clock.t()

    def service(self):
        self.t_service = self.clock.t()

    def complete(self, status: bool):
        self.status = status
        self.t_end = self.clock.t()

    def ok(self):
        self.complete(True)

    def fail(self):
        self.complete(False)

    def response_time(self):
        return self.t_end - self.t_start

    def service_time(self):
        return self.t_end - self.t_service

    def is_complete(self):
        return self.status != None


class RequestRegister:
    def __init__(self):
        self.instances: List[Request] = []
        self.clock: Clock = Clock()

    def start(self):
        r = Request(self.clock)
        self.instances.append(r)
        r.start()
        return r

    def tick(self):
        self.clock.tick()

    def n_requests(self) -> int:
        return len(self.instances)

class Worker:
    def __init__(self, service_time):
        self.t: int = 0  # clock
        self.req: t.Optional[Request] = None
        self.t_service: int = service_time
        assert self.t_service > 0

    def clear(self):
        self.t = 0
        self.req = None

    def is_free(self) -> bool:
        return self.req == None

    def assign(self, r: Request):
        assert self.req == None
        self.t = self.t_service
        self.req = r
        r.service()

    def tick(self) -> t.Optional[Request]:  # returns completed request
        if self.t > 1:
            self.t -= 1
            return None
        if self.t == 1:
            assert self.req
            r = self.req
            self.clear()
            r.ok()
            return r
        return False


class QSystem:
    def __init__(self, service_time: int, workers: int = 1):
        self.q: t.List[Request] = []
        self.worker: t.List[Worker] = [Worker(service_time) for _ in range(workers)]
        self.wmax: int = 0
        # max index of an active worker
        self._n_serviced: int = 0
        self._n_completed: int = 0

    def submit(self, req: Request):
        self.q.append(req)

    def qsize(self):
        return len(self.q)

    def n_serviced(self):
        return self._n_serviced

    def n_completed(self):
        return self._n_completed

    def tick(self):
        # The tick function is split into work assign phase and the work completion phase, since we
        # have to give the QSystem the chance to advance the clock in between those two phases, in
        # order for the service time to be captured accurately.
        self.tick_assign()
        return self.tick_complete()

    def tick_assign(self):
        for wi, w in enumerate(self.worker):
            if not self.q and wi > self.wmax:
                break
            if w.is_free() and self.q:
                w.assign(self.q.pop())
                self._n_serviced += 1
                self.wmax = max(self.wmax, wi)

    def tick_complete(self) -> t.List[Request]:
        ret: t.List[Request] = []
        for wi, w in enumerate(self.worker):
            if not self.q and wi > self.wmax:
                break
            if r := w.tick():
                self._n_completed += 1
                ret.append(r)
                if self.wmax == wi:
                    while self.wmax > 0 and self.worker[self.wmax].is_free():
                        self.wmax -= 1
        return ret



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
    n_requests: Metric = Metric()
    n_serviced: Metric = Metric()
    n_completed: Metric = Metric()
    response_time: Histogram = Histogram()
    service_time: Histogram = Histogram()

    def tick(self):
        self.n_requests.tick()
        self.n_serviced.tick()
        self.n_completed.tick()
        self.response_time.tick()
        self.service_time.tick()

    def df(self, percentiles : t.List[float] = [100]) -> pd.DataFrame:
        df = pd.DataFrame({
            "requests" : self.n_requests.data,
            "serviced" : self.n_serviced.data,
            "completed" : self.n_completed.data,
        })
        df['pending'] = df.requests - df.completed
        df['queued'] = df.requests - df.serviced
        df["r_request"] = df.requests.diff().fillna(df.requests)
        df["r_serviced"] = df.completed.diff().fillna(df.serviced)
        df["r_completed"] = df.completed.diff().fillna(df.completed)
        rts = [ np.array(times) for times in self.response_time.data ]
        sts = [ np.array(times) for times in self.service_time.data ]
        for p in percentiles:
            df[ "response_time_p{:g}".format(p) ] = [ (np.percentile(ar, p) if len(ar) > 0 else None) for ar in rts  ]
            df[ "service_time_p{:g}".format(p) ] = [ (np.percentile(ar, p) if len(ar) > 0 else None) for ar in sts  ]
        return df

def run(
    workload: t.List[int],
    sys: QSystem,
    step: int = 10,
    percentiles: t.List[float] = [1],
) -> t.Tuple[RequestRegister, RunStats]:

    reg = RequestRegister()
    stats = RunStats()
    for t, y in enumerate(workload):
        requests = int(y)
        for _ in range(requests):
            sys.submit(reg.start())

        # simulation step
        sys.tick_assign()
        reg.tick()
        completed = sys.tick_complete()

        # Collect telemetry
        stats.n_requests.add(requests)
        stats.n_completed.add(len(completed))
        stats.response_time.add([c.response_time() for c in completed])
        stats.service_time.add([c.service_time() for c in completed])
        if (t+1) % step == 0:
            stats.n_serviced.set(sys.n_serviced())
            stats.tick()

    return (reg, stats)
