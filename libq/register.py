import typing as t


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
