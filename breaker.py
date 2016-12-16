#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import threading
import types
from datetime import datetime, timedelta
from functools import wraps

import grpc


class CircuitBreakerError(Exception):
    pass


class CircuitBreaker(object):

    def __init__(self, fail_max=10, reset_timeout=30):
        self._lock = threading.RLock()
        self._fail_counter = 0
        self._state = CircuitClosedState(self)
        self._fail_max = fail_max
        self._reset_timeout = reset_timeout
        
    def is_circuit_error(self, e):
        typ = type(e)
        return issubclass(typ, grpc.RpcError) and e.code() in [
            grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED]

    def call(self, func, *args, **kwargs):
        with self._lock:
            return self._state.call(func, *args, **kwargs)

    def open(self):
        logging.info('---- open')
        with self._lock:
            self._state = CircuitOpenState(self, self._state)

    def half_open(self):
        logging.info('---- half open')
        with self._lock:
            self._state = CircuitHalfOpenState(self, self._state)

    def close(self):
        logging.info('---- close')
        with self._lock:
            self._state = CircuitClosedState(self, self._state)

    def __call__(self, func):
        @wraps(func)
        def _wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return _wrapper


class CircuitBreakerState(object):

    def __init__(self, breaker, name):
        self._breaker = breaker
        self._name = name

    @property
    def name(self):
        return self._name

    def _handle_error(self, e):
        if self._breaker.is_circuit_error(e):
            self._breaker._fail_counter += 1
            self.on_failure(e)
        else:
            self._handle_success()
        raise e

    def _handle_success(self):
        self._breaker._fail_counter = 0
        self.on_success()

    def call(self, func, *args, **kwargs):
        ret = None
        self.before_call(func, *args, **kwargs)
        try:
            ret = func(*args, **kwargs)
            if isinstance(ret, types.GeneratorType):
                return self.generator_call(ret)
        except BaseException as e:
            self._handle_error(e)
        else:
            self._handle_success()
        return ret

    def generator_call(self, wrapped_generator):
        try:
            value = yield next(wrapped_generator)
            while True:
                value = yield wrapped_generator.send(value)
        except StopIteration:
            self._handle_success()
            raise
        except BaseException as e:
            self._handle_error(e)

    def before_call(self, func, *args, **kwargs):
        pass

    def on_success(self):
        pass

    def on_failure(self, err):
        pass


class CircuitClosedState(CircuitBreakerState):

    def __init__(self, breaker, prev_state=None):
        super(CircuitClosedState, self).__init__(breaker, 'closed')
        self._breaker._fail_counter = 0

    def on_failure(self, exc):
        if self._breaker._fail_counter >= self._breaker._fail_max:
            self._breaker.open()

            error_msg = 'Failures threshold reached, circuit breaker opened'
            raise CircuitBreakerError(error_msg)


class CircuitOpenState(CircuitBreakerState):
    
    def __init__(self, breaker, prev_state=None):
        super(CircuitOpenState, self).__init__(breaker, 'open')
        self.opened_at = datetime.now()

    def before_call(self, func, *args, **kwargs):
        timeout = timedelta(seconds=self._breaker._reset_timeout)
        if datetime.now() < self.opened_at + timeout:
            error_msg = 'Timeout not elapsed yet, circuit breaker still open'
            raise CircuitBreakerError(error_msg)
        else:
            self._breaker.half_open()
            self._breaker.call(func, *args, **kwargs)


class CircuitHalfOpenState(CircuitBreakerState):

    def __init__(self, breaker, prev_state=None):
        super(CircuitHalfOpenState, self).__init__(breaker, 'half-open')

    def on_failure(self, e):
        self._breaker.open()
        raise CircuitBreakerError('Trial call failed, circuit breaker opened')

    def on_success(self):
        self._breaker.close()
