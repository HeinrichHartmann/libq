import numpy as np
import typing as t
from collections import deque

from .stats import RunStats
from .register import RequestRegister, Request, Clock


class Worker:
    def clear(self):
        raise NotImplementedError

    def is_free(self) -> bool:
        raise NotImplementedError

    def assign(self, r: Request):
        raise NotImplementedError

    def tick(self) -> t.Optional[Request]:  # returns completed request
        raise NotImplementedError


class ConstWorker(Worker):
    def __init__(self, service_time: int):
        self.t: int = 0  # clock
        self.req: t.Optional[Request] = None
        self.service_time: int = service_time
        assert self.service_time > 0

    def clear(self):
        self.t = 0
        self.req = None

    def is_free(self) -> bool:
        return self.req == None

    def assign(self, r: Request):
        assert self.req == None
        self.t = self.service_time
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


class MarkovWorker(Worker):
    def __init__(self, service_time: float, dist=np.random.geometric):
        assert service_time > 0
        self.t: int = 0  # clock
        self.req: t.Optional[Request] = None
        self.p: float = 1.0 / service_time
        self.dist = dist

    def clear(self):
        self.t = 0
        self.req = None

    def is_free(self) -> bool:
        return self.req == None

    def assign(self, r: Request):
        assert self.req == None
        self.t = self.dist(self.p)
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
    def __init__(self, workers: t.List[Worker], discipline="fifo"):
        self.q: t.List[Request] = deque()
        self.workers: t.List[Worker] = workers
        self.wmax: int = 0
        self.discipline = discipline
        # max index of an active worker
        self._n_arrived: int = 0
        self._n_serviced: int = 0
        self._n_completed: int = 0

    def qsize(self):
        return len(self.q)

    def n_arrived(self):
        return self._n_arrived

    def n_serviced(self):
        return self._n_serviced

    def n_completed(self):
        return self._n_completed

    def is_empty(self) -> int:
        return 0 == self._n_arrived - self._n_completed

    def submit(self, req: Request):
        self._n_arrived += 1
        self.q.append(req)

    def tick(self):
        # The tick function is split into work assign phase and the work completion phase,
        # since we have to give the QSystem the chance to advance the clock in between
        # those two phases, in order for the service time to be captured accurately.
        self.tick_assign()
        return self.tick_complete()

    def tick_assign(self):
        for wi, w in enumerate(self.workers):
            if not self.q and wi > self.wmax:
                break
            if w.is_free() and self.q:
                if self.discipline == "fifo":
                    w.assign(self.q.popleft())
                elif self.discipline == "lifo":
                    w.assign(self.q.pop())
                else:
                    raise Exception("Unknown discipline")
                self._n_serviced += 1
                self.wmax = max(self.wmax, wi)

    def tick_complete(self) -> t.List[Request]:
        ret: t.List[Request] = []
        for wi, w in enumerate(self.workers):
            if wi > self.wmax:
                break
            if r := w.tick():
                self._n_completed += 1
                ret.append(r)
                if self.wmax == wi:
                    while self.wmax > 0 and self.workers[self.wmax].is_free():
                        self.wmax -= 1
        return ret


def run(
    workload: t.List[int],
    sys: QSystem,
    step: int = 10,
    percentiles: t.List[float] = [1],
    deplete: bool = False,
) -> RunStats:

    reg = RequestRegister()
    stats = RunStats()
    t = -1
    # When depleted=True we will wait several step intervals until we stop
    # processing. The term variable facilitates this dalay.
    term = 0
    while True:
        if term == 1:
            break
        t += 1
        if t < len(workload):
            requests = int(workload[t])
        elif deplete and not sys.is_empty():
            requests = 0
        elif deplete:
            if term == 0:
                term = 4
            requests = 0
        else:
            break

        # Submit new requests
        for _ in range(requests):
            sys.submit(reg.start())

        # Simulation step
        sys.tick_assign()
        reg.tick()

        # Collect telemetry
        stats.c_pending.add(float(sys.n_arrived() - sys.n_completed()) / step)
        stats.c_qsize.add(float(sys.n_arrived() - sys.n_serviced()) / step)
        stats.c_util.add(float(sys.n_serviced() - sys.n_completed()) / step)
        if (t + 1) % step == 0:
            if term > 1:
                term -= 1
            stats.n_requests.set(sys.n_arrived())
            stats.n_completed.set(sys.n_completed())
            stats.n_serviced.set(sys.n_serviced())
            stats.tick()

        completed = sys.tick_complete()
        stats.response_time.add([c.response_time() for c in completed])
        stats.service_time.add([c.service_time() for c in completed])

    return stats
