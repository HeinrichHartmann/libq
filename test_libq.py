import pytest
import numpy as np

import libq

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
    W = [ libq.ConstWorker(1) ]
    SYS = libq.QSystem(W)
    for _ in range(5):
        SYS.submit(RR.start())
    assert RR.n_requests() == 5
    for i in range(5):
        RR.tick()
        SYS.tick()

def test_qs_31():
    RR = libq.RequestRegister()
    W = [ libq.ConstWorker(3) for _ in range(1) ]
    SYS = libq.QSystem(W)
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
    W = [ libq.ConstWorker(1) for _ in range(3) ]
    SYS = libq.QSystem(W)
    for _ in range(5):
        SYS.submit(RR.start())
    assert RR.n_requests() == 5
    RR.tick()
    SYS.tick()
    RR.tick()
    SYS.tick()


def test_run():
    W = [ libq.ConstWorker(1) for _ in range(1) ]
    sys = libq.QSystem(W)
    stats = libq.run([1,3,2,0,0,0,0,0], sys, step=1)
    assert stats.n_requests.data  == [1,4,6,6,6,6,6,6]
    assert stats.n_serviced.data  == [1,2,3,4,5,6,6,6]
    assert stats.n_completed.data == [1,2,3,4,5,6,6,6]
    assert stats.response_time.data == [[1],[1],[2],[3],[3],[4],[], []]
    assert stats.service_time.data == [[1],[1],[1],[1],[1],[1],[],[]]

@pytest.mark.skip(reason="For performance testing only")
def test_run_perf():
    W = [ libq.ConstWorker(30) for _ in range(500) ]
    SYS = libq.QSystem(W)
    workload = ([10]*100 + [30]*100 + [5]*500) * 50
    stats = libq.run(workload, SYS, step=10)
    print(stats)
