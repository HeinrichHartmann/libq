import libq
import numpy as np

def test_rr():
    RR = libq.RequestRegister()
    RR.tick()
    r = RR.start()
    assert RR.n_requests() == 1
    RR.tick()
    RR.tick()
    r.service()
    RR.tick()
    assert not r.is_complete()
    r.ok()
    assert r.is_complete()
    assert r.response_time() == 3
    assert r.service_time() == 1
    assert RR.n_requests() == 1
    r = RR.start()
    assert RR.n_requests() == 2
    RR.tick()
    assert not r.is_complete()
    r.fail()
    assert r.is_complete()


def test_qs_11():
    RR = libq.RequestRegister()
    SYS = libq.QSystem(service_time = 1, workers = 1)
    for _ in range(5):
        SYS.submit(RR.start())
    assert RR.n_requests() == 5
    for i in range(5):
        RR.tick()
        SYS.tick()

def test_qs_31():
    RR = libq.RequestRegister()
    SYS = libq.QSystem(service_time = 3, workers = 1)
    for _ in range(5):
        SYS.submit(RR.start())
    assert RR.n_requests() == 5
    assert SYS.qsize() == 5
    for i in range(5):
        assert SYS.qsize() == 5 - i
        RR.tick()
        SYS.tick()
        RR.tick()
        SYS.tick()
        RR.tick()
        SYS.tick()


def test_qs_13():
    RR = libq.RequestRegister()
    SYS = libq.QSystem(service_time = 1, workers = 3)
    for _ in range(5):
        SYS.submit(RR.start())
    assert RR.n_requests() == 5
    RR.tick()
    SYS.tick()
    RR.tick()
    SYS.tick()


def test_run():
    sys = libq.QSystem(service_time = 1, workers = 1)
    reg, stats = libq.run([1,3,2,0,0,0,0,0], sys, step=1)
    assert stats.n_requests.data  == [1,4,6,6,6,6,6,6]
    assert stats.n_serviced.data  == [1,2,3,4,5,6,6,6]
    # assert stats.n_pending.data   == [0,2,3,2,1,0,0,0]
    assert stats.n_completed.data == [1,2,3,4,5,6,6,6]
    assert stats.response_time.data == [[1],[1],[1],[2],[4],[5],[],[]]
    assert stats.service_time.data == [[1],[1],[1],[1],[1],[1],[],[]]
    assert stats.qsize.data == [0,2,3,2,1,0,0,0]


def test_run_perf():
    sys = libq.QSystem(service_time = 30, workers = 500)
    workload = ([10]*100 + [30]*100 + [5]*500) * 50
    reg, stats = libq.run(workload, sys, step=10)
    print(stats)
