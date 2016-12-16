#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from collections import deque
from datetime import datetime, timedelta

import grpc

from xrpc.breaker import CircuitBreaker, CircuitBreakerError

BREAKER_MAX_FAIL_COUNT = 10
BREAKER_RESET_PERIOD = 30

def error_hanlder(func):
    cb = CircuitBreaker(BREAKER_MAX_FAIL_COUNT, BREAKER_RESET_PERIOD)

    def _(*args, **kwargs):
        try:
            return cb.call(func, *args, **kwargs)
        except grpc._channel._Rendezvous as e :
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                logging.info('---- retry...')
                return cb.call(func, *args, **kwargs)
            else:
                raise e
    return _


def enhance(stub):
    for name, fn in stub.__dict__.iteritems():
        setattr(stub, name, error_hanlder(fn))
    return stub


def new_stub(cls, channel):
    stub = cls(channel)
    return enhance(stub)

def new_lb_stub(cls, channels):
    #stubs = [cls(c) for c in channels]
    stubs = [new_stub(cls, c) for c in channels]
    stub_pool = StubPool(stubs)
    return stub_pool


# ------------------ Load Balance ---------------------

class StubPool():

    def __init__(self, stubs):
        self._active_stubs = deque(stubs)
        self._break_stubs = deque()

        for fn_name in stubs[0].__dict__.keys():
            setattr(self, fn_name, self.distribute(fn_name))

    def distribute(self, fn_name):
        def _(*args, **kwargs):
            def rpc(stub, is_breaked_stub):
                method = getattr(stub, fn_name, None)
                try:
                    ret = method(*args, **kwargs)
                except CircuitBreakerError:
                    self._break_stubs.append((stub, datetime.now()))
                    if is_breaked_stub and len(self._active_stubs) > 0:
                        stub = self._active_stubs.popleft()
                        return rpc(stub, False)
                    else:
                        raise
                except:
                    self._active_stubs.append(stub)
                    raise
                else:
                    self._active_stubs.append(stub)
                    return ret
            
            is_breaked_stub = False
            if len(self._break_stubs) > 0:
                _, breaked_at = self._break_stubs[0]
                if datetime.now() > breaked_at + timedelta(seconds=BREAKER_RESET_PERIOD):
                    is_breaked_stub = True
                    stub, _ = self._break_stubs.popleft()
            if not is_breaked_stub:
                if len(self._active_stubs) == 0:
                    raise CircuitBreakerError('All stubs are circuit breaked')
                stub = self._active_stubs.popleft()

            return rpc(stub, is_breaked_stub)

        return _